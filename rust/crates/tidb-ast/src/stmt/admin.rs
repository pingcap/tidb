use crate::util::{escape_string_literal, push_name_path};
use crate::{
    AddQueryWatchStmt, AnalyzeIncrementalStmt, AnalyzeTableStmt, BinlogStmt, BrieStmt,
    CalibrateResourceStmt, CreateBindingStmt, CreateStatisticsStmt, DescribeTableStmt,
    DropBindingStmt, DropQueryWatchStmt, ExplainForStmt, ExplainStmt, Expr, FlushStmt, KillStmt,
    RecommendIndexStmt, RefreshStatsStmt, ServerControlStmt, SetBindingStmt, SetConfigStmt,
    ShowBindingsStmt, ShowCharsetStmt, ShowCollationStmt, ShowColumnsStmt, ShowCreateKind,
    ShowDatabasesStmt, ShowDistributionJobsStmt, ShowEnginesStmt, ShowErrorsStmt, ShowIndexStmt,
    ShowInspectionStmt, ShowMaskingPoliciesStmt, ShowOpenTablesStmt, ShowPlacementStmt,
    ShowProfileStmt, ShowStatsBucketsStmt, ShowStatsHistogramsStmt, ShowStatsLockedStmt,
    ShowStatsTopNStmt, ShowStatusStmt, ShowTableNextRowIdStmt, ShowTablePlacementStmt,
    ShowTableStatusStmt, ShowTablesStmt, ShowWarningsStmt, SplitRegionStmt, StatsLockStmt, Stmt,
    TrafficStmt, UserSpec,
};

#[path = "admin/ddl_job_alter.rs"]
mod ddl_job_alter;
#[path = "admin/ddl_job_control.rs"]
mod ddl_job_control;
#[path = "admin/flush_plan_cache.rs"]
mod flush_plan_cache;
#[path = "admin/role_grant.rs"]
mod role_grant;

pub use ddl_job_alter::{AdminAlterDdlJobOption, AdminAlterDdlJobsStmt};
pub use ddl_job_control::{AdminDdlJobControlKind, AdminDdlJobControlStmt};
pub use flush_plan_cache::AdminPlanCacheScope;
pub use role_grant::{GrantRoleStmt, RevokeRoleStmt};

/// Administrative, inspection, and diagnostics statements behind
/// [`crate::Stmt::Admin`].
#[derive(Debug, Clone, PartialEq)]
pub enum AdminStmt {
    /// `GRANT privilege [, ...] ON [TABLE|FUNCTION|PROCEDURE] level TO user [, ...]`.
    ///
    /// This is deliberately the privilege-grant form only. Role grants and
    /// account authentication/TLS/resource options carry distinct Go AST
    /// payloads and must receive their own typed statements rather than being
    /// silently folded into a privilege grant.
    Grant(Box<GrantStmt>),
    /// `GRANT PROXY ON proxied_user TO proxy_user [, ...] [WITH GRANT OPTION]`.
    GrantProxy(Box<GrantProxyStmt>),
    /// `GRANT role [, ...] TO user [, ...]`.
    ///
    /// Go represents role membership separately from privileges. Keeping the
    /// payload distinct prevents `ON` levels, grant options, and account
    /// authentication from becoming invalid states here.
    GrantRole(Box<GrantRoleStmt>),
    /// `REVOKE privilege [, ...] ON [TABLE|FUNCTION|PROCEDURE] level FROM user [, ...]`.
    ///
    /// This is the `ON ... FROM ...` privilege form. Dynamic privileges are
    /// carried by the typed `GrantPrivilege::dynamic` marker; role revocation
    /// and `REVOKE ALL, GRANT OPTION FROM ...` remain separate Go paths.
    Revoke(Box<RevokeStmt>),
    /// `REVOKE role [, ...] FROM user [, ...]`.
    RevokeRole(Box<RevokeRoleStmt>),
    /// `SHOW GRANTS [FOR user [USING role, ...]]`.
    ShowGrants(Box<ShowGrantsStmt>),
    /// `SHOW MASTER STATUS`.
    ///
    /// The result is sourced from the server's binlog position and GTID
    /// state, which remains an executor boundary in this seed.
    ShowMasterStatus,
    /// `SHOW PRIVILEGES`.
    ///
    /// The result depends on TiDB's privilege registry and compatibility
    /// rules, so parsing is kept distinct while execution remains explicit.
    ShowPrivileges,
    /// `SHOW BUILTINS`.
    ///
    /// Builtin-function metadata is owned by the parser/executor registry,
    /// which remains outside this seed. Keep this as a distinct Go SHOW kind
    /// so it cannot be confused with privilege or table inspection.
    ShowBuiltins,
    /// Backup, restore, and BR job commands.
    Brie(Box<BrieStmt>),
    /// `TRACE ... statement`.
    Trace(Box<crate::TraceStmt>),
    /// `EXPLAIN ... FOR CONNECTION id`.
    ExplainFor(Box<ExplainForStmt>),
    /// Internal `BINLOG` command.
    Binlog(Box<BinlogStmt>),
    /// `KILL` connection/query command.
    Kill(Box<KillStmt>),
    /// `SET CONFIG` cluster configuration command.
    SetConfig(Box<SetConfigStmt>),
    /// `RECOMMEND INDEX` command family.
    RecommendIndex(Box<RecommendIndexStmt>),
    /// `CREATE STATISTICS` extended statistics command.
    CreateStatistics(Box<CreateStatisticsStmt>),
    /// `DROP STATISTICS name`.
    DropStatistics(String),
    /// `SHUTDOWN`, `RESTART`, or `HELP`.
    ServerControl(Box<ServerControlStmt>),
    /// `CANCEL DISTRIBUTION JOB id`.
    CancelDistributionJob(i64),
    /// `CALIBRATE RESOURCE`.
    CalibrateResource(Box<CalibrateResourceStmt>),
    /// `QUERY WATCH ADD ...`.
    AddQueryWatch(Box<AddQueryWatchStmt>),
    /// `QUERY WATCH REMOVE ...`.
    DropQueryWatch(Box<DropQueryWatchStmt>),
    /// `CANCEL IMPORT JOB id`.
    CancelImportJob(i64),
    /// `SHOW [RAW] IMPORT JOB[S] [id | WHERE expression]`.
    ShowImportJobs(Box<ShowImportJobsStmt>),
    /// `SHOW IMPORT GROUP[S] ['group-key' | WHERE expression]`.
    ShowImportGroups(Box<ShowImportGroupsStmt>),
    /// `FLUSH STATUS`, `FLUSH PRIVILEGES`, or
    /// `FLUSH TABLE[S] [table [, ...]] [WITH READ LOCK]`.
    ///
    /// Binlog-modifier, plugin, log, and statistics forms carry separate Go
    /// state and are deliberately not folded into this payload.
    Flush(Box<FlushStmt>),
    /// `ADMIN FLUSH [SESSION | INSTANCE | GLOBAL] PLAN_CACHE`.
    ///
    /// This is unrelated to ordinary `FLUSH` commands: Go owns it in the
    /// ADMIN statement family and carries an explicit statement scope.
    FlushPlanCache(AdminPlanCacheScope),
    /// `DO expr [, expr ...]`.
    ///
    /// Go owns this in its simple-statement executor, but it parses alongside
    /// the other non-query administrative commands. The expressions are
    /// retained in source order because their evaluation side effects and
    /// warning behavior are part of the eventual execution contract.
    Do(Vec<Expr>),
    /// A value-less `ADMIN RELOAD` control command.
    Reload(AdminReloadKind),
    /// `ADMIN SET BDR ROLE {PRIMARY | SECONDARY}`.
    SetBdrRole(BdrRole),
    /// `ADMIN UNSET BDR ROLE`.
    UnsetBdrRole,
    /// `ADMIN SHOW BDR ROLE`.
    ShowBdrRole,
    /// `ADMIN SHOW SLOW {RECENT | TOP [INTERNAL | ALL]} count`.
    ShowSlow(Box<AdminShowSlowStmt>),
    /// Value-less `ADMIN SHOW DDL` job-owner inspection command.
    ///
    /// The `JOBS` and `JOB QUERIES` extensions have distinct Go AST types and
    /// remain separate variants below.
    ShowDdl,
    /// `ADMIN SHOW DDL JOBS [number] [WHERE expression]`.
    ///
    /// This is distinct from the value-less `ADMIN SHOW DDL` command and
    /// `ADMIN SHOW DDL JOB QUERIES`, whose result payloads differ in Go.
    ShowDdlJobs(Box<AdminShowDdlJobsStmt>),
    /// `ADMIN SHOW DDL JOB QUERIES ids` or `... QUERIES LIMIT ...`.
    ///
    /// Go assigns distinct statement types to the ID-list and range forms,
    /// so the typed payload retains that split instead of using optional
    /// compatibility fields.
    ShowDdlJobQueries(Box<AdminShowDdlJobQueriesStmt>),
    /// `ADMIN {CANCEL | PAUSE | RESUME} DDL JOBS id [, id ...]`.
    DdlJobControl(Box<AdminDdlJobControlStmt>),
    /// `ADMIN ALTER DDL JOBS job_id option = literal [, ...]`.
    ///
    /// The parser preserves Go's ordered, lower-cased option payload; DDL
    /// queue mutation remains an executor boundary in the seed.
    AlterDdlJobs(Box<AdminAlterDdlJobsStmt>),
    /// `ADMIN SHOW table NEXT_ROW_ID`.
    ShowNextRowId(Box<AdminShowNextRowIdStmt>),
    /// `SHOW CREATE {TABLE | VIEW | SEQUENCE | DATABASE} name`.
    ShowCreate {
        /// The object kind whose definition is requested.
        kind: ShowCreateKind,
        /// Whether `IF NOT EXISTS` was written for a database.
        if_not_exists: bool,
        /// The object's name path.
        name: Vec<String>,
    },
    /// `SHOW CREATE USER user[@host]` or `SHOW CREATE USER CURRENT_USER`.
    ///
    /// Go stores this target as an account identity rather than a table name,
    /// so it stays separate from the other `SHOW CREATE` object kinds.
    ShowCreateUser(UserSpec),
    /// `SHOW [SESSION | GLOBAL] VARIABLES [LIKE <expr> | WHERE <expr>]`.
    ShowVariables {
        /// `GLOBAL` scope; `false` is session scope.
        global: bool,
        /// The decoded `LIKE` pattern.
        like: Option<String>,
        /// Optional full-expression filter, mutually exclusive with `LIKE`.
        where_clause: Option<Expr>,
    },
    /// `SHOW [GLOBAL | SESSION] STATUS [LIKE <expr> | WHERE <expr>]`.
    ///
    /// Status variables have an independent Go AST kind and session/global
    /// scope semantics, so they are not collapsed into `SHOW VARIABLES`.
    ShowStatus(Box<ShowStatusStmt>),
    /// `SHOW WARNINGS [LIKE <expr> | WHERE <expr>]`.
    ShowWarnings(Box<ShowWarningsStmt>),
    /// `SHOW [COUNT(*)] ERRORS [LIKE <expr> | WHERE <expr>]`.
    ShowErrors(Box<ShowErrorsStmt>),
    /// `SHOW COLLATION [LIKE <expr> | WHERE <expr>]`.
    ///
    /// The catalog is not represented by the seed executor, but the filter
    /// grammar is kept separate from `SHOW WARNINGS`: Go owns distinct AST
    /// statement types and the result schemas are unrelated.
    ShowCollation(Box<ShowCollationStmt>),
    /// `SHOW ENGINES [LIKE <expr> | WHERE <expr>]`.
    ///
    /// The result depends on the server's storage-engine registry, which
    /// remains an executor boundary in this seed.
    ShowEngines(Box<ShowEnginesStmt>),
    /// `SHOW CHARACTER SET` / `SHOW CHARSET` [LIKE <expr> | WHERE <expr>].
    ///
    /// Go restores all three spellings (`CHARACTER SET`, `CHAR SET`, and
    /// `CHARSET`) to canonical `SHOW CHARSET` and carries the shared
    /// LIKE/WHERE filter payload.
    ShowCharset(Box<ShowCharsetStmt>),
    /// `SHOW STATS_HISTOGRAMS [LIKE <simple expression> | WHERE <expression>]`.
    ///
    /// TiDB's histogram rows need statistics metadata at execution, but the
    /// shared Go SHOW filter grammar is still source-visible parser state.
    ShowStatsHistograms(Box<ShowStatsHistogramsStmt>),
    /// `SHOW STATS_BUCKETS [LIKE <simple expression> | WHERE <expression>]`.
    ///
    /// Its histogram-bucket result rows remain distinct from the sibling
    /// statistics SHOW payloads and are not synthesized by this seed.
    ShowStatsBuckets(Box<ShowStatsBucketsStmt>),
    /// `SHOW STATS_LOCKED [LIKE <simple expression> | WHERE <expression>]`.
    ///
    /// Go maps this to `mysql.stats_table_locked`, whose lock metadata and
    /// privilege filtering remain an execution boundary.
    ShowStatsLocked(Box<ShowStatsLockedStmt>),
    /// `SHOW STATS_TOPN [LIKE <simple expression> | WHERE <expression>]`.
    ///
    /// Go gives TopN statistics its own SHOW type and result layout, so it is
    /// distinct from STATS_HISTOGRAMS and STATS_BUCKETS.
    ShowStatsTopN(Box<ShowStatsTopNStmt>),
    /// `SHOW DATABASES [LIKE <simple expression> | WHERE <expression>]`.
    ///
    /// The result depends on TiDB's schema visibility and privilege model,
    /// which remains an executor boundary. The filter is nevertheless a
    /// typed parser/restore payload rather than discarded trailing SQL.
    ShowDatabases(Box<ShowDatabasesStmt>),
    /// `SHOW TABLES [LIKE <simple expression>]`.
    ///
    /// The full Go grammar additionally carries `FULL`, a schema scope, and
    /// a `WHERE` predicate. Those need their own typed contract and remain
    /// intentionally outside this narrow parser/restore slice.
    ShowTables(Box<ShowTablesStmt>),
    /// `SHOW OPEN TABLES [IN | FROM schema]`.
    ///
    /// Go assigns this an independent statement kind and metadata result
    /// shape, so it is not folded into plural `SHOW TABLES`.
    ShowOpenTables(Box<ShowOpenTablesStmt>),
    /// `SHOW TABLE STATUS [FROM | IN database] [LIKE <expr> | WHERE <expr>]`.
    ///
    /// It has a distinct Go AST kind and information-schema result layout, so
    /// it remains separate from both plural `SHOW TABLES` and singular
    /// `SHOW TABLE name NEXT_ROW_ID`.
    ShowTableStatus(Box<ShowTableStatusStmt>),
    /// `SHOW TABLE name NEXT_ROW_ID`.
    ShowTableNextRowId(Box<ShowTableNextRowIdStmt>),
    /// `SHOW COLUMNS {FROM | IN} table [LIKE <expr> | WHERE <expr>]`.
    ShowColumns(Box<ShowColumnsStmt>),
    /// `SHOW {INDEX | INDEXES | KEYS} {FROM | IN} table [LIKE <expr> | WHERE <expr>]`.
    ///
    /// This retains the parser/restore contract only. The seed executor has
    /// no index metadata catalog and rejects execution explicitly.
    ShowIndex(Box<ShowIndexStmt>),
    /// Ordinary SHOW targets sharing TiDB's common filter payload.
    ShowInspection(Box<ShowInspectionStmt>),
    /// `SHOW DISTRIBUTION JOB[S]`.
    ShowDistributionJobs(Box<ShowDistributionJobsStmt>),
    /// `SHOW TABLE ... REGIONS|DISTRIBUTIONS`.
    ShowTablePlacement(Box<ShowTablePlacementStmt>),
    /// `SHOW PLACEMENT ...`.
    ShowPlacement(Box<ShowPlacementStmt>),
    /// `SHOW PROFILE ...`.
    ShowProfile(Box<ShowProfileStmt>),
    /// `SHOW MASKING POLICIES FOR ...`.
    ShowMaskingPolicies(Box<ShowMaskingPoliciesStmt>),
    /// A SQL binding definition.
    CreateBinding(Box<CreateBindingStmt>),
    /// A SQL binding removal.
    DropBinding(Box<DropBindingStmt>),
    /// A SQL binding enable/disable command.
    SetBinding(Box<SetBindingStmt>),
    /// A SQL binding listing command.
    ShowBindings(Box<ShowBindingsStmt>),
    /// `ANALYZE TABLE`.
    AnalyzeTable(Box<AnalyzeTableStmt>),
    /// `ANALYZE INCREMENTAL TABLE`.
    AnalyzeIncremental(Box<AnalyzeIncrementalStmt>),
    /// Traffic capture, replay, and job inspection/control.
    Traffic(Box<TrafficStmt>),
    /// `REFRESH STATS` for table, database, or global targets.
    RefreshStats(Box<RefreshStatsStmt>),
    /// `ADMIN CHECK TABLE` or `ADMIN CHECK INDEX`.
    AdminCheck(Box<AdminCheckStmt>),
    /// `ADMIN CHECKSUM TABLE table [, table ...]`.
    AdminChecksum(Box<AdminChecksumStmt>),
    /// `ADMIN RECOVER INDEX table index`.
    AdminRecoverIndex(Box<AdminRecoverIndexStmt>),
    /// `ADMIN CLEANUP INDEX table index`.
    AdminCleanupIndex(Box<AdminRecoverIndexStmt>),
    /// `ADMIN PLUGINS {ENABLE | DISABLE} plugin [, plugin ...]`.
    Plugins {
        /// Whether plugins are enabled rather than disabled.
        enable: bool,
        /// Plugin names in source order.
        plugins: Vec<String>,
    },
    /// One of Go's value-less binding maintenance commands.
    BindingControl(AdminBindingControlKind),
    /// `ADMIN CREATE WORKLOAD SNAPSHOT`.
    CreateWorkloadSnapshot,
    /// `ADMIN CLEANUP TABLE LOCK table [, table ...]`.
    ///
    /// TiDB's cleanup operation releases stale metadata locks owned by the
    /// named tables. The parser/restore contract is kept separate from
    /// `ADMIN CLEANUP INDEX`: Go uses a distinct `CleanupTableLockStmt` AST
    /// payload and executor path.
    CleanupTableLock(Box<AdminCleanupTableLockStmt>),
    /// `LOCK STATS table [, table ...] [PARTITION ...]`.
    LockStats(Box<StatsLockStmt>),
    /// `UNLOCK STATS table [, table ...] [PARTITION ...]`.
    UnlockStats(Box<StatsLockStmt>),
    /// An `EXPLAIN` wrapper retaining its cross-domain inner statement.
    Explain(Box<ExplainStmt>),
    /// `PLAN REPLAYER DUMP EXPLAIN` around a typed query.
    ///
    /// This narrow wrapper deliberately excludes the other Plan Replayer
    /// command families. They carry files, raw SQL lists, historical-stats
    /// expressions, and capture state which need their own faithful AST
    /// contracts instead of being silently discarded here.
    PlanReplayer(Box<PlanReplayerStmt>),
    /// `DESC`/`DESCRIBE` or the `EXPLAIN <table>` fallback.
    DescribeTable(Box<DescribeTableStmt>),
    /// `LOAD STATS 'path'`.
    ///
    /// TiDB owns this in `ast/stats.go`, separately from `LOAD DATA`: it
    /// imports optimizer statistics rather than table rows.
    LoadStats(Box<LoadStatsStmt>),
    /// `DROP STATS table [, ...] [GLOBAL | PARTITION name [, ...]]`.
    DropStats(Box<DropStatsStmt>),
    /// `SPLIT [REGION FOR] [PARTITION] TABLE ...`.
    ///
    /// Go owns this under its region-management statement family.  It is not
    /// catalog DDL and needs real TiKV placement/storage semantics, so this
    /// project keeps it in the administrative envelope and rejects execution
    /// explicitly.
    SplitRegion(Box<SplitRegionStmt>),
}

impl AdminStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Grant(grant) => grant.restore_into(out),
            Self::GrantProxy(grant) => grant.restore_into(out),
            Self::GrantRole(grant) => grant.restore_into(out),
            Self::Revoke(revoke) => revoke.restore_into(out),
            Self::RevokeRole(revoke) => revoke.restore_into(out),
            Self::ShowGrants(show) => show.restore_into(out),
            Self::Flush(flush) => flush.restore_into(out),
            Self::FlushPlanCache(scope) => scope.restore_into(out),
            Self::Do(exprs) => {
                out.push_str("DO ");
                for (index, expr) in exprs.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    expr.restore_into(out);
                }
            }
            Self::Reload(kind) => {
                out.push_str("ADMIN RELOAD ");
                out.push_str(kind.restore_name());
            }
            Self::SetBdrRole(role) => {
                out.push_str("ADMIN SET BDR ROLE ");
                out.push_str(match role {
                    BdrRole::Primary => "PRIMARY",
                    BdrRole::Secondary => "SECONDARY",
                });
            }
            Self::UnsetBdrRole => out.push_str("ADMIN UNSET BDR ROLE"),
            Self::ShowBdrRole => out.push_str("ADMIN SHOW BDR ROLE"),
            Self::ShowSlow(show) => show.restore_into(out),
            Self::ShowDdl => out.push_str("ADMIN SHOW DDL"),
            Self::ShowDdlJobs(show) => show.restore_into(out),
            Self::ShowDdlJobQueries(show) => show.restore_into(out),
            Self::DdlJobControl(control) => control.restore_into(out),
            Self::AlterDdlJobs(alter) => alter.restore_into(out),
            Self::ShowNextRowId(show) => show.restore_into(out),
            Self::ShowTableNextRowId(show) => show.restore_into(out),
            Self::ShowCreate {
                kind,
                if_not_exists,
                name,
            } => {
                out.push_str("SHOW CREATE ");
                out.push_str(match kind {
                    ShowCreateKind::Table => "TABLE ",
                    ShowCreateKind::View => "VIEW ",
                    ShowCreateKind::Sequence => "SEQUENCE ",
                    ShowCreateKind::Database => "DATABASE ",
                    ShowCreateKind::Procedure => "PROCEDURE ",
                    ShowCreateKind::PlacementPolicy => "PLACEMENT POLICY ",
                    ShowCreateKind::ResourceGroup => "RESOURCE GROUP ",
                });
                if *if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                push_name_path(out, name);
            }
            Self::ShowCreateUser(user) => {
                out.push_str("SHOW CREATE USER ");
                user.restore_into(out);
            }
            Self::ShowMasterStatus => out.push_str("SHOW MASTER STATUS"),
            Self::ShowPrivileges => out.push_str("SHOW PRIVILEGES"),
            Self::ShowBuiltins => out.push_str("SHOW BUILTINS"),
            Self::Brie(brie) => brie.restore_into(out),
            Self::Trace(statement) => statement.restore_into(out),
            Self::ExplainFor(statement) => statement.restore_into(out),
            Self::Binlog(statement) => statement.restore_into(out),
            Self::Kill(statement) => statement.restore_into(out),
            Self::SetConfig(statement) => statement.restore_into(out),
            Self::RecommendIndex(statement) => statement.restore_into(out),
            Self::CreateStatistics(statement) => statement.restore_into(out),
            Self::DropStatistics(name) => {
                out.push_str("DROP STATISTICS ");
                out.push_str(&crate::util::back_quote(name));
            }
            Self::ServerControl(statement) => statement.restore_into(out),
            Self::CancelDistributionJob(job_id) => {
                out.push_str("CANCEL DISTRIBUTION JOB ");
                out.push_str(&job_id.to_string());
            }
            Self::CalibrateResource(statement) => statement.restore_into(out),
            Self::AddQueryWatch(statement) => statement.restore_into(out),
            Self::DropQueryWatch(statement) => statement.restore_into(out),
            Self::CancelImportJob(job_id) => {
                out.push_str("CANCEL IMPORT JOB ");
                out.push_str(&job_id.to_string());
            }
            Self::ShowImportJobs(show) => show.restore_into(out),
            Self::ShowImportGroups(show) => show.restore_into(out),
            Self::ShowVariables {
                global,
                like,
                where_clause,
            } => {
                out.push_str(if *global {
                    "SHOW GLOBAL VARIABLES"
                } else {
                    "SHOW SESSION VARIABLES"
                });
                if let Some(pattern) = like {
                    out.push_str(" LIKE ");
                    out.push_str(&crate::restore_string_literal(pattern));
                }
                if let Some(where_clause) = where_clause {
                    out.push_str(" WHERE ");
                    where_clause.restore_into(out);
                }
            }
            Self::ShowStatus(show) => show.restore_into(out),
            Self::ShowWarnings(show) => show.restore_into(out),
            Self::ShowErrors(show) => show.restore_into(out),
            Self::ShowCollation(show) => show.restore_into(out),
            Self::ShowEngines(show) => show.restore_into(out),
            Self::ShowCharset(show) => show.restore_into(out),
            Self::ShowStatsHistograms(show) => show.restore_into(out),
            Self::ShowStatsBuckets(show) => show.restore_into(out),
            Self::ShowStatsLocked(show) => show.restore_into(out),
            Self::ShowStatsTopN(show) => show.restore_into(out),
            Self::ShowDatabases(show) => show.restore_into(out),
            Self::ShowTables(show) => show.restore_into(out),
            Self::ShowOpenTables(show) => show.restore_into(out),
            Self::ShowTableStatus(show) => show.restore_into(out),
            Self::ShowColumns(show) => show.restore_into(out),
            Self::ShowIndex(show) => show.restore_into(out),
            Self::ShowInspection(show) => show.restore_into(out),
            Self::ShowDistributionJobs(show) => show.restore_into(out),
            Self::ShowTablePlacement(show) => show.restore_into(out),
            Self::ShowPlacement(show) => show.restore_into(out),
            Self::ShowProfile(show) => show.restore_into(out),
            Self::ShowMaskingPolicies(show) => show.restore_into(out),
            Self::CreateBinding(binding) => binding.restore_into(out),
            Self::DropBinding(binding) => binding.restore_into(out),
            Self::SetBinding(binding) => binding.restore_into(out),
            Self::ShowBindings(show) => show.restore_into(out),
            Self::AnalyzeTable(analyze) => analyze.restore_into(out),
            Self::AnalyzeIncremental(analyze) => analyze.restore_into(out),
            Self::Traffic(traffic) => traffic.restore_into(out),
            Self::RefreshStats(refresh) => refresh.restore_into(out),
            Self::AdminCheck(check) => check.restore_into(out),
            Self::AdminChecksum(checksum) => checksum.restore_into(out),
            Self::AdminRecoverIndex(recover) => recover.restore_into(out),
            Self::AdminCleanupIndex(cleanup) => cleanup.restore_cleanup_into(out),
            Self::Plugins { enable, plugins } => {
                out.push_str(if *enable {
                    "ADMIN PLUGINS ENABLE"
                } else {
                    "ADMIN PLUGINS DISABLE"
                });
                for (index, plugin) in plugins.iter().enumerate() {
                    out.push_str(if index == 0 { " " } else { ", " });
                    out.push_str(plugin);
                }
            }
            Self::BindingControl(kind) => {
                out.push_str("ADMIN ");
                out.push_str(kind.restore_name());
                out.push_str(" BINDINGS");
            }
            Self::CreateWorkloadSnapshot => out.push_str("ADMIN CREATE WORKLOAD SNAPSHOT"),
            Self::CleanupTableLock(cleanup) => cleanup.restore_into(out),
            Self::LockStats(lock) => lock.restore_into(out, true),
            Self::UnlockStats(lock) => lock.restore_into(out, false),
            Self::Explain(explain) => explain.restore_into(out),
            Self::PlanReplayer(replayer) => replayer.restore_into(out),
            Self::DescribeTable(describe) => describe.restore_into(out),
            Self::LoadStats(load) => load.restore_into(out),
            Self::DropStats(drop) => drop.restore_into(out),
            Self::SplitRegion(split) => split.restore_into(out),
        }
    }
}

/// Go's import-job inspection payload on `ShowStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowImportJobsStmt {
    /// Whether `RAW` was written before `IMPORT`.
    pub raw: bool,
    /// Singular job ID; absent for the plural listing form.
    pub job_id: Option<i64>,
    /// Optional plural-list filter.
    pub where_clause: Option<Expr>,
}

impl ShowImportJobsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ");
        if self.raw {
            out.push_str("RAW ");
        }
        if let Some(job_id) = self.job_id {
            out.push_str("IMPORT JOB ");
            out.push_str(&job_id.to_string());
        } else {
            out.push_str("IMPORT JOBS");
            if let Some(where_clause) = &self.where_clause {
                out.push_str(" WHERE ");
                where_clause.restore_into(out);
            }
        }
    }
}

/// Go's import-group inspection payload on `ShowStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowImportGroupsStmt {
    /// Singular group key; absent for the plural listing form.
    pub group_key: Option<String>,
    /// Optional plural/singular filter.
    pub where_clause: Option<Expr>,
}

impl ShowImportGroupsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW IMPORT ");
        if let Some(group_key) = &self.group_key {
            out.push_str("GROUP '");
            out.push_str(&escape_string_literal(group_key));
            out.push('\'');
        } else {
            out.push_str("GROUPS");
        }
        if let Some(where_clause) = &self.where_clause {
            out.push_str(" WHERE ");
            where_clause.restore_into(out);
        }
    }
}

/// Go's `ADMIN SHOW table NEXT_ROW_ID` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminShowNextRowIdStmt {
    /// The table whose next row ID is requested.
    pub table: Vec<String>,
}

impl AdminShowNextRowIdStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW ");
        push_name_path(out, &self.table);
        out.push_str(" NEXT_ROW_ID");
    }
}

/// Go's `AdminShowDDLJobs` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminShowDdlJobsStmt {
    /// Optional positive-looking integer token represented by Go's zero-value
    /// field. A source `0` is restored as omitted, matching Go's AST restore.
    pub job_number: i64,
    /// Optional DDL-job metadata predicate.
    pub where_clause: Option<Expr>,
}

impl AdminShowDdlJobsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW DDL JOBS");
        if self.job_number != 0 {
            out.push(' ');
            out.push_str(&self.job_number.to_string());
        }
        if let Some(where_clause) = &self.where_clause {
            out.push_str(" WHERE ");
            where_clause.restore_into(out);
        }
    }
}

/// The distinct Go payload alternatives for `ADMIN SHOW DDL JOB QUERIES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminShowDdlJobQueriesStmt {
    /// `ADMIN SHOW DDL JOB QUERIES id [, id ...]`.
    JobIds(Vec<i64>),
    /// `ADMIN SHOW DDL JOB QUERIES LIMIT {count | offset, count | count OFFSET offset}`.
    ///
    /// Go restores every spelling as `LIMIT offset, count`.
    Limit {
        /// Row offset, defaulting to zero for the one-number form.
        offset: u64,
        /// Number of job-query rows to return.
        count: u64,
    },
}

impl AdminShowDdlJobQueriesStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW DDL JOB QUERIES ");
        match self {
            Self::JobIds(job_ids) => {
                for (index, job_id) in job_ids.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&job_id.to_string());
                }
            }
            Self::Limit { offset, count } => {
                out.push_str("LIMIT ");
                out.push_str(&offset.to_string());
                out.push_str(", ");
                out.push_str(&count.to_string());
            }
        }
    }
}

/// Go's `ADMIN SHOW SLOW` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminShowSlowStmt {
    /// The selected recent/top result set.
    pub mode: AdminShowSlowMode,
    /// Maximum number of slow statements to list.
    pub count: u64,
}

/// The mutually exclusive `ADMIN SHOW SLOW` modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminShowSlowMode {
    /// `RECENT count`.
    Recent,
    /// `TOP [INTERNAL | ALL] count`.
    Top(AdminShowSlowTopScope),
}

/// The optional scope after `ADMIN SHOW SLOW TOP`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminShowSlowTopScope {
    /// Omitted scope.
    Default,
    /// `INTERNAL` statements only.
    Internal,
    /// Both internal and user statements.
    All,
}

impl AdminShowSlowStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN SHOW SLOW ");
        match self.mode {
            AdminShowSlowMode::Recent => out.push_str("RECENT "),
            AdminShowSlowMode::Top(scope) => {
                out.push_str("TOP ");
                match scope {
                    AdminShowSlowTopScope::Default => {}
                    AdminShowSlowTopScope::Internal => out.push_str("INTERNAL "),
                    AdminShowSlowTopScope::All => out.push_str("ALL "),
                }
            }
        }
        out.push_str(&self.count.to_string());
    }
}

/// Go's value-less `AdminReload*` variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminReloadKind {
    /// `STATISTICS` or `STATS_EXTENDED`, restoring as `STATS_EXTENDED`.
    Statistics,
    /// `OPT_RULE_BLACKLIST`.
    OptRuleBlacklist,
    /// `EXPR_PUSHDOWN_BLACKLIST`.
    ExprPushdownBlacklist,
    /// `BINDINGS`.
    Bindings,
    /// `CLUSTER [BINDINGS]`, restoring as `CLUSTER BINDINGS`.
    ClusterBindings,
}

/// Go's value-less ADMIN binding maintenance operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminBindingControlKind {
    /// Flush in-memory binding state.
    Flush,
    /// Capture statements into bindings.
    Capture,
    /// Evolve existing bindings.
    Evolve,
}

impl AdminBindingControlKind {
    fn restore_name(self) -> &'static str {
        match self {
            Self::Flush => "FLUSH",
            Self::Capture => "CAPTURE",
            Self::Evolve => "EVOLVE",
        }
    }
}

impl AdminReloadKind {
    fn restore_name(self) -> &'static str {
        match self {
            Self::Statistics => "STATS_EXTENDED",
            Self::OptRuleBlacklist => "OPT_RULE_BLACKLIST",
            Self::ExprPushdownBlacklist => "EXPR_PUSHDOWN_BLACKLIST",
            Self::Bindings => "BINDINGS",
            Self::ClusterBindings => "CLUSTER BINDINGS",
        }
    }
}

/// The two `ADMIN CHECK` forms that share a prefix but have different
/// physical-index contracts in TiDB.
#[derive(Debug, Clone, PartialEq)]
pub enum AdminCheckStmt {
    /// `ADMIN CHECK TABLE table [, table ...]`.
    ///
    /// Go's parser permits a list even though its planner later rejects more
    /// than one table for the actual consistency-check operation.
    Table {
        /// Table-name paths in source order.
        tables: Vec<Vec<String>>,
    },
    /// `ADMIN CHECK INDEX table index [(begin, end), ...]`.
    Index {
        /// Checked table's dotted name path.
        table: Vec<String>,
        /// Parsed index identifier, restored as a bare name by Go.
        index: String,
        /// Optional half-open handle intervals.
        handle_ranges: Vec<AdminCheckHandleRange>,
    },
}

/// One half-open handle range attached to [`AdminCheckStmt::Index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminCheckHandleRange {
    /// Inclusive lower handle bound.
    pub begin: i64,
    /// Exclusive upper handle bound.
    pub end: i64,
}

impl AdminCheckStmt {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Table { tables } => {
                out.push_str("ADMIN CHECK TABLE ");
                for (i, table) in tables.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    push_name_path(out, table);
                }
            }
            Self::Index {
                table,
                index,
                handle_ranges,
            } => {
                out.push_str("ADMIN CHECK INDEX ");
                push_name_path(out, table);
                // Go restores AdminStmt.Index as a bare identifier even when
                // the input index name used backticks.
                out.push(' ');
                out.push_str(index);
                for (i, range) in handle_ranges.iter().enumerate() {
                    if i == 0 {
                        out.push(' ');
                    } else {
                        out.push_str(", ");
                    }
                    out.push('(');
                    out.push_str(&range.begin.to_string());
                    out.push(',');
                    out.push_str(&range.end.to_string());
                    out.push(')');
                }
            }
        }
    }
}

/// Go's `AdminChecksumTable` payload. This is deliberately separate from
/// [`AdminCheckStmt::Table`]: a checksum scans TiKV key ranges and returns
/// aggregate CRC/KV/byte rows, while an admin check validates index records.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminChecksumStmt {
    /// Table-name paths in source order.
    pub tables: Vec<Vec<String>>,
}

impl AdminChecksumStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN CHECKSUM TABLE ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
    }
}

/// Go's `AdminRecoverIndex` payload. Recovery is separate from `ADMIN CHECK
/// INDEX`: it backfills a corrupted secondary index and returns recovery
/// counts instead of validating existing key/value records.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminRecoverIndexStmt {
    /// Recovered table's dotted name path.
    pub table: Vec<String>,
    /// Index identifier, restored bare by Go's AST.
    pub index: String,
}

/// Go's `CleanupTableLockStmt` payload for stale table-lock cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminCleanupTableLockStmt {
    /// Table-name paths in source order.
    pub tables: Vec<Vec<String>>,
}

impl AdminCleanupTableLockStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN CLEANUP TABLE LOCK ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
    }
}

impl AdminRecoverIndexStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN RECOVER INDEX ");
        push_name_path(out, &self.table);
        out.push(' ');
        out.push_str(&self.index);
    }

    fn restore_cleanup_into(&self, out: &mut String) {
        out.push_str("ADMIN CLEANUP INDEX ");
        push_name_path(out, &self.table);
        out.push(' ');
        out.push_str(&self.index);
    }
}

/// Go's `ShowStmt{Tp: ShowGrants}` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowGrantsStmt {
    /// Optional account after `FOR`; absent means the current session user.
    pub user: Option<crate::UserSpec>,
    /// Optional active-role override list after `USING`.
    pub roles: Vec<crate::UserSpec>,
}

impl ShowGrantsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW GRANTS");
        if let Some(user) = &self.user {
            out.push_str(" FOR ");
            user.restore_into(out);
        }
        if !self.roles.is_empty() {
            out.push_str(" USING ");
            for (index, role) in self.roles.iter().enumerate() {
                if index != 0 {
                    out.push_str(", ");
                }
                role.restore_into(out);
            }
        }
    }
}

/// TiDB's standard privilege-revoke statement, transliterated from Go's
/// `ast.RevokeStmt` and sharing its privilege/object/level payload types with
/// [`GrantStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    /// Standard privileges in their written order.
    pub privileges: Vec<GrantPrivilege>,
    /// Optional object class after `ON`.
    pub object_type: Option<GrantObjectType>,
    /// Scope from which the privileges are revoked.
    pub level: GrantLevel,
    /// Accounts in their written order.
    pub users: Vec<crate::UserSpec>,
}

impl RevokeStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("REVOKE ");
        for (index, privilege) in self.privileges.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            privilege.restore_into(out);
        }
        out.push_str(" ON ");
        if let Some(object_type) = self.object_type {
            out.push_str(match object_type {
                GrantObjectType::Table => "TABLE ",
                GrantObjectType::Function => "FUNCTION ",
                GrantObjectType::Procedure => "PROCEDURE ",
            });
        }
        self.level.restore_into(out);
        out.push_str(" FROM ");
        for (index, user) in self.users.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
    }
}

/// TiDB's `LOAD STATS 'path'` parser/restore envelope.
///
/// Applying a statistics artifact needs TiDB's statistics handle, infoschema
/// versioning, and session domain. The seed executor rejects this before it
/// changes transaction or catalog state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStatsStmt {
    /// Decoded statistics artifact path.
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// TiDB's statistics-deletion command and its optional deprecated scopes.
pub struct DropStatsStmt {
    /// Target tables in source order.
    pub tables: Vec<Vec<String>>,
    /// Whether the deprecated `GLOBAL` scope was specified.
    pub global: bool,
    /// Optional deprecated partition names.
    pub partitions: Vec<String>,
}

impl DropStatsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("DROP STATS ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
        if self.global {
            out.push_str(" GLOBAL");
        }
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION ");
            for (index, partition) in self.partitions.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&crate::util::back_quote(partition));
            }
        }
    }
}

impl LoadStatsStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("LOAD STATS ");
        // Go's `LoadStatsStmt.Restore` uses `WriteString`, unlike ordinary
        // scalar string expressions which use `_UTF8MB4` under default flags.
        out.push('\'');
        out.push_str(&escape_string_literal(&self.path));
        out.push('\'');
    }
}

/// TiDB's core privilege-grant statement, transliterated from Go's
/// `ast.GrantStmt`, including each grantee's typed authentication payload.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    /// Privileges in their written order.
    pub privileges: Vec<GrantPrivilege>,
    /// Optional object class after `ON`.
    pub object_type: Option<GrantObjectType>,
    /// Scope to which the privileges apply.
    pub level: GrantLevel,
    /// Grantee accounts in their written order.
    pub users: Vec<crate::CreateUserSpec>,
    /// Optional `REQUIRE` TLS/authentication constraints in Go source order.
    pub tls_options: Vec<crate::AlterUserTlsOption>,
    /// `WITH GRANT OPTION`.
    pub with_grant: bool,
}

/// TiDB's special proxy-user grant, which has no privilege list or object
/// level and therefore cannot be represented by [`GrantStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct GrantProxyStmt {
    /// Account whose identity may be assumed.
    pub local_user: UserSpec,
    /// Accounts receiving proxy access.
    pub external_users: Vec<UserSpec>,
    /// Whether recipients may grant the proxy privilege onward.
    pub with_grant: bool,
}

impl GrantProxyStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("GRANT PROXY ON ");
        self.local_user.restore_into(out);
        out.push_str(" TO ");
        for (index, user) in self.external_users.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
        if self.with_grant {
            out.push_str(" WITH GRANT OPTION");
        }
    }
}

impl GrantStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("GRANT ");
        for (index, privilege) in self.privileges.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            privilege.restore_into(out);
        }
        out.push_str(" ON ");
        if let Some(object_type) = self.object_type {
            out.push_str(match object_type {
                GrantObjectType::Table => "TABLE ",
                GrantObjectType::Function => "FUNCTION ",
                GrantObjectType::Procedure => "PROCEDURE ",
            });
        }
        self.level.restore_into(out);
        out.push_str(" TO ");
        for (index, user) in self.users.iter().enumerate() {
            if index != 0 {
                out.push_str(", ");
            }
            user.restore_into(out);
        }
        if !self.tls_options.is_empty() {
            out.push_str(" REQUIRE ");
            for (index, option) in self.tls_options.iter().enumerate() {
                if index > 0 {
                    out.push_str(" AND ");
                }
                option.restore_into(out);
            }
        }
        if self.with_grant {
            out.push_str(" WITH GRANT OPTION");
        }
    }
}

/// One `ast.PrivElem`: its canonical privilege spelling and optional columns.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantPrivilege {
    /// Go-restored uppercase privilege spelling, including dynamic privileges.
    pub name: String,
    /// Optional column list attached to this privilege.
    pub columns: Vec<String>,
    /// Whether Go parsed this privilege through its identifier-only
    /// `ExtendedPriv` branch. Keeping the distinction typed lets REVOKE
    /// accept dynamic privileges without widening role or special no-`ON`
    /// forms, while preserving the same canonical restore text.
    pub dynamic: bool,
}

impl GrantPrivilege {
    fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        if !self.columns.is_empty() {
            out.push_str(" (");
            for (index, column) in self.columns.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push_str(&crate::util::back_quote(column));
            }
            out.push(')');
        }
    }
}

/// Optional object class after `ON`, matching Go's `ObjectTypeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantObjectType {
    /// `TABLE`.
    Table,
    /// `FUNCTION`.
    Function,
    /// `PROCEDURE`.
    Procedure,
}

/// Go's three ordinary `GrantLevel` restore forms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantLevel {
    /// `*.*`.
    Global,
    /// `*` or `` `database`.* ``.
    Database(Option<String>),
    /// `` `table` `` or `` `database`.`table` ``.
    Table {
        /// The optional database qualifier.
        database: Option<String>,
        /// The table name.
        table: String,
    },
}

impl GrantLevel {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Global => out.push_str("*.*"),
            Self::Database(None) => out.push('*'),
            Self::Database(Some(database)) => {
                out.push_str(&crate::util::back_quote(database));
                out.push_str(".*");
            }
            Self::Table { database, table } => {
                if let Some(database) = database {
                    out.push_str(&crate::util::back_quote(database));
                    out.push('.');
                }
                out.push_str(&crate::util::back_quote(table));
            }
        }
    }
}

/// The two cluster roles accepted by TiDB's `ADMIN SET BDR ROLE` grammar.
///
/// The empty role used by TiDB's metadata layer is represented by the
/// separate [`AdminStmt::UnsetBdrRole`] command, so an invalid role cannot be
/// constructed or restored as a successful statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BdrRole {
    /// The primary BDR cluster role.
    Primary,
    /// The secondary BDR cluster role.
    Secondary,
}

/// Complete parser-visible `PLAN REPLAYER` operation.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanReplayerStmt {
    /// `PLAN REPLAYER LOAD 'file'`.
    Load(String),
    /// `PLAN REPLAYER CAPTURE [REMOVE] 'sql-digest' 'plan-digest'`.
    Capture {
        /// Whether the capture is removed rather than added.
        remove: bool,
        /// SQL digest.
        sql_digest: String,
        /// Plan digest.
        plan_digest: String,
    },
    /// `PLAN REPLAYER [DUMP] [WITH STATS ...] EXPLAIN [ANALYZE] target`.
    Dump {
        /// Optional historical-statistics timestamp expression.
        historical_stats: Option<Box<Expr>>,
        /// Whether the target is executed while collecting the replay.
        analyze: bool,
        /// Dump target.
        target: Box<PlanReplayerTarget>,
    },
}

/// Target carried by a Plan Replayer dump.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanReplayerTarget {
    /// An ordinary parsed statement.
    Statement(Box<Stmt>),
    /// A file containing SQL.
    File(String),
    /// One or more literal SQL statements.
    Statements(Vec<String>),
    /// The special slow-query selector.
    SlowQuery {
        /// Optional predicate.
        where_clause: Option<Box<Expr>>,
        /// Optional ordering.
        order_by: Vec<crate::OrderItem>,
        /// Optional row limit.
        limit: Option<Box<crate::Limit>>,
    },
}

impl PlanReplayerStmt {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Load(file) => {
                out.push_str("PLAN REPLAYER LOAD '");
                out.push_str(&escape_string_literal(file));
                out.push('\'');
            }
            Self::Capture {
                remove,
                sql_digest,
                plan_digest,
            } => {
                out.push_str("PLAN REPLAYER CAPTURE ");
                if *remove {
                    out.push_str("REMOVE ");
                }
                out.push('\'');
                out.push_str(&escape_string_literal(sql_digest));
                out.push_str("' '");
                out.push_str(&escape_string_literal(plan_digest));
                out.push('\'');
            }
            Self::Dump {
                historical_stats,
                analyze,
                target,
            } => {
                out.push_str("PLAN REPLAYER DUMP ");
                if let Some(timestamp) = historical_stats {
                    out.push_str("WITH STATS AS OF TIMESTAMP ");
                    timestamp.restore_into(out);
                    out.push(' ');
                }
                out.push_str(if *analyze {
                    "EXPLAIN ANALYZE "
                } else {
                    "EXPLAIN "
                });
                target.restore_into(out);
            }
        }
    }
}

impl PlanReplayerTarget {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Statement(statement) => statement.restore_into(out),
            Self::File(file) => {
                out.push('\'');
                out.push_str(&escape_string_literal(file));
                out.push('\'');
            }
            Self::Statements(statements) => {
                out.push('(');
                for (index, statement) in statements.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push('\'');
                    out.push_str(&escape_string_literal(statement));
                    out.push('\'');
                }
                out.push(')');
            }
            Self::SlowQuery {
                where_clause,
                order_by,
                limit,
            } => {
                out.push_str("SLOW QUERY");
                if let Some(where_clause) = where_clause {
                    out.push_str(" WHERE ");
                    where_clause.restore_into(out);
                }
                if !order_by.is_empty() {
                    out.push_str(" ORDER BY ");
                    for (index, item) in order_by.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        item.restore_into(out);
                    }
                }
                if let Some(limit) = limit {
                    out.push_str(" LIMIT ");
                    if let Some(offset) = &limit.offset {
                        offset.restore_into(out);
                        out.push(',');
                    }
                    limit.count.restore_into(out);
                }
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AdminStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Grant(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::GrantProxy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::GrantRole(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Revoke(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RevokeRole(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowGrants(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowMasterStatus => {}
            Self::ShowPrivileges => {}
            Self::ShowBuiltins => {}
            Self::Brie(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Trace(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ExplainFor(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Binlog(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Kill(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetConfig(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RecommendIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateStatistics(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropStatistics(field_0) => {
                let _ = field_0;
            }
            Self::ServerControl(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CancelDistributionJob(field_0) => {
                let _ = field_0;
            }
            Self::CalibrateResource(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AddQueryWatch(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropQueryWatch(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CancelImportJob(field_0) => {
                let _ = field_0;
            }
            Self::ShowImportJobs(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowImportGroups(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Flush(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::FlushPlanCache(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Do(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Reload(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetBdrRole(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::UnsetBdrRole => {}
            Self::ShowBdrRole => {}
            Self::ShowSlow(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowDdl => {}
            Self::ShowDdlJobs(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowDdlJobQueries(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DdlJobControl(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterDdlJobs(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowNextRowId(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowCreate {
                kind,
                if_not_exists,
                name,
            } => {
                if !crate::Visitable::accept(kind, visitor) {
                    return false;
                }
                let _ = kind;
                let _ = if_not_exists;
                let _ = name;
            }
            Self::ShowCreateUser(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowVariables {
                global,
                like,
                where_clause,
            } => {
                if let Some(value) = where_clause.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = global;
                let _ = like;
                let _ = where_clause;
            }
            Self::ShowStatus(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowWarnings(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowErrors(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowCollation(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowEngines(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowCharset(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowStatsHistograms(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowStatsBuckets(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowStatsLocked(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowStatsTopN(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowDatabases(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowTables(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowOpenTables(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowTableStatus(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowTableNextRowId(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowColumns(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowInspection(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowDistributionJobs(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowTablePlacement(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowPlacement(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowProfile(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowMaskingPolicies(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateBinding(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropBinding(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetBinding(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ShowBindings(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AnalyzeTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AnalyzeIncremental(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Traffic(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RefreshStats(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AdminCheck(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AdminChecksum(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AdminRecoverIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AdminCleanupIndex(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Plugins { enable, plugins } => {
                let _ = enable;
                let _ = plugins;
            }
            Self::BindingControl(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::CreateWorkloadSnapshot => {}
            Self::CleanupTableLock(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::LockStats(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::UnlockStats(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Explain(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::PlanReplayer(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DescribeTable(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::LoadStats(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropStats(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SplitRegion(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowImportJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            raw,
            job_id,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = raw;
        let _ = job_id;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowImportGroupsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            group_key,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = group_key;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowNextRowIdStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table } = self;
        let _ = table;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowDdlJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            job_number,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = job_number;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowDdlJobQueriesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::JobIds(field_0) => {
                let _ = field_0;
            }
            Self::Limit { offset, count } => {
                let _ = offset;
                let _ = count;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { mode, count } = self;
        if !crate::Visitable::accept(mode, visitor) {
            return false;
        }
        let _ = mode;
        let _ = count;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Recent => {}
            Self::Top(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminShowSlowTopScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Internal => {}
            Self::All => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminReloadKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statistics => {}
            Self::OptRuleBlacklist => {}
            Self::ExprPushdownBlacklist => {}
            Self::Bindings => {}
            Self::ClusterBindings => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminBindingControlKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Flush => {}
            Self::Capture => {}
            Self::Evolve => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminCheckStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table { tables } => {
                let _ = tables;
            }
            Self::Index {
                table,
                index,
                handle_ranges,
            } => {
                for value in handle_ranges.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = table;
                let _ = index;
                let _ = handle_ranges;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminCheckHandleRange {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { begin, end } = self;
        let _ = begin;
        let _ = end;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminChecksumStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { tables } = self;
        let _ = tables;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminRecoverIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, index } = self;
        let _ = table;
        let _ = index;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminCleanupTableLockStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { tables } = self;
        let _ = tables;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowGrantsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { user, roles } = self;
        if let Some(value) = user.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in roles.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = user;
        let _ = roles;
        visitor.leave(self)
    }
}

impl crate::Visitable for RevokeStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            privileges,
            object_type,
            level,
            users,
        } = self;
        for value in privileges.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = object_type.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(level, visitor) {
            return false;
        }
        for value in users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = privileges;
        let _ = object_type;
        let _ = level;
        let _ = users;
        visitor.leave(self)
    }
}

impl crate::Visitable for LoadStatsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { path } = self;
        let _ = path;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropStatsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            tables,
            global,
            partitions,
        } = self;
        let _ = tables;
        let _ = global;
        let _ = partitions;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            privileges,
            object_type,
            level,
            users,
            tls_options,
            with_grant,
        } = self;
        for value in privileges.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = object_type.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(level, visitor) {
            return false;
        }
        for value in users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in tls_options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = privileges;
        let _ = object_type;
        let _ = level;
        let _ = users;
        let _ = tls_options;
        let _ = with_grant;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantProxyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            local_user,
            external_users,
            with_grant,
        } = self;
        if !crate::Visitable::accept(local_user, visitor) {
            return false;
        }
        for value in external_users.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = local_user;
        let _ = external_users;
        let _ = with_grant;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantPrivilege {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            columns,
            dynamic,
        } = self;
        let _ = name;
        let _ = columns;
        let _ = dynamic;
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantObjectType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table => {}
            Self::Function => {}
            Self::Procedure => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for GrantLevel {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Global => {}
            Self::Database(field_0) => {
                let _ = field_0;
            }
            Self::Table { database, table } => {
                let _ = database;
                let _ = table;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BdrRole {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Primary => {}
            Self::Secondary => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PlanReplayerStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Load(field_0) => {
                let _ = field_0;
            }
            Self::Capture {
                remove,
                sql_digest,
                plan_digest,
            } => {
                let _ = remove;
                let _ = sql_digest;
                let _ = plan_digest;
            }
            Self::Dump {
                historical_stats,
                analyze,
                target,
            } => {
                if let Some(value) = historical_stats.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                if !crate::Visitable::accept(target.as_mut(), visitor) {
                    return false;
                }
                let _ = historical_stats;
                let _ = analyze;
                let _ = target;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PlanReplayerTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statement(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::File(field_0) => {
                let _ = field_0;
            }
            Self::Statements(field_0) => {
                let _ = field_0;
            }
            Self::SlowQuery {
                where_clause,
                order_by,
                limit,
            } => {
                if let Some(value) = where_clause.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                for value in order_by.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if let Some(value) = limit.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                let _ = where_clause;
                let _ = order_by;
                let _ = limit;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
