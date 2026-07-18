use crate::util::{escape_string_literal, push_name_path};
use crate::{
    AnalyzeIncrementalStmt, AnalyzeTableStmt, CreateBindingStmt, DescribeTableStmt,
    DropBindingStmt, ExplainStmt, Expr, FlushStmt, QueryStmt, RefreshStatsStmt, SetBindingStmt,
    ShowBindingsStmt, ShowCharsetStmt, ShowCollationStmt, ShowColumnsStmt, ShowCreateKind,
    ShowDatabasesStmt, ShowEnginesStmt, ShowErrorsStmt, ShowIndexStmt, ShowOpenTablesStmt,
    ShowStatsBucketsStmt, ShowStatsHistogramsStmt, ShowStatsLockedStmt, ShowStatsTopNStmt,
    ShowStatusStmt, ShowTableNextRowIdStmt, ShowTableStatusStmt, ShowTablesStmt, ShowWarningsStmt,
    SplitRegionStmt, StatsLockStmt, TrafficStmt, UserSpec,
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
    /// `FLUSH STATUS`, `FLUSH PRIVILEGES`, or
    /// `FLUSH TABLE[S] [table [, ...]] [WITH READ LOCK]`.
    ///
    /// Binlog-modifier, plugin, log, and statistics forms carry separate Go
    /// state and are deliberately not folded into this payload.
    Flush(Box<FlushStmt>),
    /// `ADMIN FLUSH {SESSION | GLOBAL} PLAN_CACHE`.
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
    PlanReplayerDumpExplain(Box<PlanReplayerDumpExplainStmt>),
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
            Self::CleanupTableLock(cleanup) => cleanup.restore_into(out),
            Self::LockStats(lock) => lock.restore_into(out, true),
            Self::UnlockStats(lock) => lock.restore_into(out, false),
            Self::Explain(explain) => explain.restore_into(out),
            Self::PlanReplayerDumpExplain(replayer) => replayer.restore_into(out),
            Self::DescribeTable(describe) => describe.restore_into(out),
            Self::LoadStats(load) => load.restore_into(out),
            Self::DropStats(drop) => drop.restore_into(out),
            Self::SplitRegion(split) => split.restore_into(out),
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

/// The source-backed `PLAN REPLAYER DUMP EXPLAIN <query>` envelope.
///
/// Go's Plan Replayer parser delegates the trailing SQL to the ordinary
/// statement parser. The one checked integration row is a `WITH` query with
/// a set operation, so retaining the existing typed [`QueryStmt`] directly
/// keeps its CTE/set-operation tree and canonical restore intact. This is not
/// a generic Plan Replayer representation: file, string-list, slow-query,
/// `ANALYZE`, `LOAD`, and `CAPTURE` forms intentionally require separate
/// AST variants before they can be accepted.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanReplayerDumpExplainStmt {
    /// The query passed to Plan Replayer's dump operation.
    pub query: Box<QueryStmt>,
}

impl PlanReplayerDumpExplainStmt {
    fn restore_into(&self, out: &mut String) {
        out.push_str("PLAN REPLAYER DUMP EXPLAIN ");
        self.query.restore_into(out);
    }
}
