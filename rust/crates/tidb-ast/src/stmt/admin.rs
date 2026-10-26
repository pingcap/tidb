//! Administrative, inspection, and diagnostics statements behind
//! [`crate::Stmt::Admin`]. The `AdminStmt` dispatch enum and its restore live
//! here; each payload family lives in a sibling module below.

use crate::util::{escape_string_literal, push_name_path};
use crate::{
    AddQueryWatchStmt, AnalyzeIncrementalStmt, AnalyzeTableStmt, BinlogStmt, BrieStmt,
    CalibrateResourceStmt, CreateBindingStmt, CreateStatisticsStmt, DescribeTableStmt,
    DropBindingStmt, DropQueryWatchStmt, ExplainForStmt, ExplainStmt, Expr, FlushStmt, KillStmt,
    RecommendIndexStmt, RefreshStatsStmt, ServerControlStmt, SetBindingStmt, SetConfigStmt,
    SplitRegionStmt,
    StatsLockStmt, Stmt, TrafficStmt, UserSpec,
};

#[path = "admin/check.rs"]
mod check;
#[path = "admin/ddl_job_alter.rs"]
mod ddl_job_alter;
#[path = "admin/ddl_job_control.rs"]
mod ddl_job_control;
#[path = "admin/flush_plan_cache.rs"]
mod flush_plan_cache;
#[path = "admin/grant.rs"]
mod grant;
#[path = "admin/plan_replayer.rs"]
mod plan_replayer;
#[path = "admin/role_grant.rs"]
mod role_grant;
#[path = "admin/show.rs"]
mod show;
#[path = "admin/stats.rs"]
mod stats;

pub use check::{
    AdminCheckHandleRange, AdminCheckStmt, AdminChecksumStmt, AdminCleanupTableLockStmt,
    AdminRecoverIndexStmt,
};
pub use ddl_job_alter::{AdminAlterDdlJobOption, AdminAlterDdlJobsStmt};
pub use ddl_job_control::{AdminDdlJobControlKind, AdminDdlJobControlStmt};
pub use flush_plan_cache::AdminPlanCacheScope;
pub use grant::{
    GrantLevel, GrantObjectType, GrantPrivilege, GrantProxyStmt, GrantStmt, RevokeStmt,
    };
pub use plan_replayer::{PlanReplayerStmt, PlanReplayerTarget};
pub use role_grant::{GrantRoleStmt, RevokeRoleStmt};
pub use show::{
    AdminBindingControlKind, AdminReloadKind, AdminShowDdlJobQueriesStmt, AdminShowDdlJobsStmt,
    AdminShowNextRowIdStmt, AdminShowSlowMode, AdminShowSlowStmt, AdminShowSlowTopScope,
    };
pub use stats::{DropStatsStmt, LoadStatsStmt};

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
    /// Go `ast.ShowStmt`: every SHOW kind the parser has moved onto Go's
    /// one-node shape.
    Show(Box<crate::ShowStmt>),
    /// A SQL binding definition.
    CreateBinding(Box<CreateBindingStmt>),
    /// A SQL binding removal.
    DropBinding(Box<DropBindingStmt>),
    /// A SQL binding enable/disable command.
    SetBinding(Box<SetBindingStmt>),
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
            Self::Show(show) => show.restore_into(out),
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
            Self::CreateBinding(binding) => binding.restore_into(out),
            Self::DropBinding(binding) => binding.restore_into(out),
            Self::SetBinding(binding) => binding.restore_into(out),
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

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AdminStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Show(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
            }
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
// END GENERATED AST VISITOR IMPLEMENTATIONS
