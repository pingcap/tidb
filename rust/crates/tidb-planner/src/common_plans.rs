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

//! Go `base.Plan` as `PlanBuilder.Build` and `planner.Optimize` return it,
//! and the plans of `pkg/planner/core/common_plans.go` that are not
//! operator trees.
//!
//! SEED: the common plans grow here as the statements that build them move
//! onto the planner.

use tidb_datatype::FieldName;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use tidb_planner_core_operator_logicalop::LogicalPlan;
use crate::physical::PhysicalPlan;

/// Go `ShowNextRowID` (`common_plans.go:83`): `SHOW TABLE t NEXT_ROW_ID`,
/// with `buildShowNextRowID`'s schema.
#[derive(Clone, Debug)]
pub struct ShowNextRowID {
    /// Go `TableName`, as its name path (`schema, table` once preprocessed).
    pub table_name: Vec<String>,
    /// Go `SimpleSchemaProducer`'s schema.
    pub schema: Schema,
    /// Go `SimpleSchemaProducer`'s output names.
    pub names: Vec<FieldName>,
}

/// Go `expression.VarAssignment`: one assignment of a planned `SET`.
#[derive(Clone, Debug)]
pub struct VarAssignment {
    /// Go `Name`, as the statement spelled it (a system variable's name is
    /// already lowercased by the parser).
    pub name: String,
    /// Go `Expr`: the rewritten value; `None` exactly when `is_default`.
    pub expr: Option<Expression>,
    /// Go `IsDefault`.
    pub is_default: bool,
    /// Go `IsGlobal`.
    pub is_global: bool,
    /// Go `IsSystem`.
    pub is_system: bool,
    /// Go `IsInstance`.
    pub is_instance: bool,
    /// Go `ExtendValue`'s string: `SET NAMES cs COLLATE co`'s collation.
    pub extend_value: Option<String>,
}

/// Go `Set` (`common_plans.go:256`): the plan of a `SET` statement.
#[derive(Clone, Debug, Default)]
pub struct Set {
    /// Go `VarAssigns`.
    pub var_assigns: Vec<VarAssignment>,
}

/// Go `Simple` (`common_plans.go`): a statement `SimpleExec` runs, as
/// `buildSimple` admitted it.
#[derive(Clone, Debug)]
pub struct Simple {
    /// Go `Statement`.
    pub statement: tidb_ast::Stmt,
    /// Go `StaleTxnStartTS`, for `BEGIN ... AS OF`.
    pub stale_txn_start_ts: u64,
}

/// Go `DDL` (`common_plans.go`): a DDL statement `DDLExec` runs.
#[derive(Clone, Debug)]
pub struct Ddl {
    /// Go `Statement`.
    pub statement: tidb_ast::Stmt,
}

/// Go `SQLBindOpType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlBindOp {
    /// Go `OpSQLBindCreate` (and `buildCreateBindPlanFromPlanDigest`'s
    /// `OpSQLBindCreateByPlanDigest`).
    Create,
    /// Go `OpSQLBindDrop` / `OpSQLBindDropByDigest`.
    Drop,
    /// Go `OpFlushBindings`.
    FlushBindings,
    /// Go `OpReloadBindings`.
    ReloadBindings,
    /// Go `OpReloadClusterBindings`.
    ReloadClusterBindings,
    /// Go `OpSetBindingStatus` / `OpSetBindingStatusByDigest`.
    SetBindingStatus,
}

/// Go `SQLBindPlan`: a binding statement for `SQLBindExec`, carrying the
/// statement it was built from.
#[derive(Clone, Debug)]
pub struct SqlBindPlan {
    /// Go `SQLBindOp`.
    pub op: SqlBindOp,
    /// The binding statement (`CREATE` / `DROP` / `SET BINDING`), `None` for
    /// the ADMIN binding maintenance ops.
    pub statement: Option<tidb_ast::AdminStmt>,
}

/// Go `base.Plan`: an operator tree before or after optimization, or one of
/// the common plans.
#[derive(Clone, Debug)]
pub enum Plan {
    /// A logical operator tree (Go `base.LogicalPlan`).
    Logical(LogicalPlan),
    /// A physical operator tree (Go `base.PhysicalPlan`).
    Physical(PhysicalPlan),
    /// Go `ShowNextRowID`.
    ShowNextRowID(ShowNextRowID),
    /// Go `Set`.
    Set(Set),
    /// Go `Simple`.
    Simple(Box<Simple>),
    /// Go `DDL`.
    Ddl(Box<Ddl>),
    /// Go `CheckTable` / `CheckIndexRange` (`ADMIN CHECK TABLE | INDEX`).
    CheckTable(Box<tidb_ast::AdminCheckStmt>),
    /// Go `RecoverIndex`.
    RecoverIndex(Box<tidb_ast::AdminRecoverIndexStmt>),
    /// Go `CleanupIndex`.
    CleanupIndex(Box<tidb_ast::AdminRecoverIndexStmt>),
    /// Go `ChecksumTable`.
    ChecksumTable(Box<tidb_ast::AdminChecksumStmt>),
    /// Go `ShowDDL`.
    ShowDdl,
    /// Go `ShowDDLJobQueries` / `ShowDDLJobQueriesWithRange`.
    ShowDdlJobQueries(Box<tidb_ast::AdminShowDdlJobQueriesStmt>),
    /// Go `ShowSlow`.
    ShowSlow(Box<tidb_ast::AdminShowSlowStmt>),
    /// Go `CancelDDLJobs` / `PauseDDLJobs` / `ResumeDDLJobs`.
    DdlJobControl(Box<tidb_ast::AdminDdlJobControlStmt>),
    /// Go `AlterDDLJob`.
    AlterDdlJob(Box<tidb_ast::AdminAlterDdlJobsStmt>),
    /// Go `ReloadExprPushdownBlacklist`.
    ReloadExprPushdownBlacklist,
    /// Go `ReloadOptRuleBlacklist`.
    ReloadOptRuleBlacklist,
    /// Go `AdminPlugins`.
    AdminPlugins {
        /// Go `Action == Enable`.
        enable: bool,
        /// Go `Plugins`.
        plugins: Vec<String>,
    },
    /// Go `AdminShowBDRRole`.
    AdminShowBdrRole,
    /// Go `WorkloadRepoCreate`.
    WorkloadRepoCreate,
    /// Go `SQLBindPlan`.
    SqlBind(Box<SqlBindPlan>),
    /// Go `LoadStats`.
    LoadStats(Box<tidb_ast::LoadStatsStmt>),
    /// Go `LockStats`.
    LockStats(Box<tidb_ast::StatsLockStmt>),
    /// Go `UnlockStats`.
    UnlockStats(Box<tidb_ast::StatsLockStmt>),
    /// Go `Traffic`.
    Traffic(Box<tidb_ast::TrafficStmt>),
    /// Go `PlanReplayer`.
    PlanReplayer(Box<tidb_ast::PlanReplayerStmt>),
    /// Go `SetConfig`.
    SetConfig(Box<tidb_ast::SetConfigStmt>),
    /// Go `SplitRegion` / `SplitIndexRegion`.
    SplitRegion(Box<tidb_ast::SplitRegionStmt>),
    /// Go `Analyze`.
    Analyze(Box<tidb_ast::AdminStmt>),
    /// Go `RecommendIndexPlan`.
    RecommendIndex(Box<tidb_ast::RecommendIndexStmt>),
    /// Go `CompactTable` (`buildCompactTable`), over the preprocessed
    /// statement's table.
    CompactTable(Box<tidb_ast::CompactTableStmt>),
    /// Go `Explain` (`buildExplain`).
    Explain(Box<tidb_ast::ExplainStmt>),
    /// Go `ExplainFor` (`buildExplainFor`).
    ExplainFor(Box<tidb_ast::ExplainForStmt>),
    /// Go `Trace` (`buildTrace`).
    Trace(Box<tidb_ast::TraceStmt>),
}
