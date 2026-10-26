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

//! Ports of `pkg/parser/ast/sem_test.go` (origin/master).
//!
//! Go walks integer `ShowStmtType` / `AdminStmtType` / `BRIEKind` domains and
//! asserts `SEMCommand() != "UNKNOWN"`. Rust's closed enums make an omitted
//! mapping a compile error; these tables pin every source kind's command
//! string against `Stmt::sem_command`.

use tidb_ast::{
    AdminAlterDdlJobsStmt, AdminBindingControlKind, AdminCheckHandleRange, AdminCheckStmt,
    AdminChecksumStmt, AdminDdlJobControlKind, AdminDdlJobControlStmt, AdminPlanCacheScope,
    AdminRecoverIndexStmt, AdminReloadKind, AdminShowDdlJobQueriesStmt, AdminShowDdlJobsStmt,
    AdminShowNextRowIdStmt, AdminShowSlowMode, AdminShowSlowStmt, AdminStmt, BdrRole,
    BrieKind, BrieStmt, NodeBox, Stmt, UserSpec,
};

fn unknown_command() -> &'static str {
    "UNKNOWN"
}

fn admin(stmt: AdminStmt) -> Stmt {
    Stmt::Admin(NodeBox::new(stmt))
}

fn user() -> UserSpec {
    UserSpec {
        current_user: false,
        user: "u".to_string(),
        host: "%".to_string(),
    }
}

fn show(tp: tidb_ast::ShowStmtType) -> AdminStmt {
    AdminStmt::Show(Box::new(tidb_ast::ShowStmt::new(tp)))
}

/// `pkg/parser/ast/sem_test.go::TestShowCommand`.
///
/// Go iterates `ShowStmtType` from 1 to `showTpCount-1` on a zero `ShowStmt`
/// and requires a non-UNKNOWN SEM command. Rust has no integer SHOW domain
/// every source SHOW kind is constructed below.
#[test]
fn show_command() {
    let cases: Vec<(AdminStmt, &str)> = vec![
        (
            show(tidb_ast::ShowStmtType::Engines),
            "SHOW ENGINES",
        ),
        (
            show(tidb_ast::ShowStmtType::Databases),
            "SHOW DATABASES",
        ),
        (
            show(tidb_ast::ShowStmtType::Tables),
            "SHOW TABLE",
        ),
        (
            show(tidb_ast::ShowStmtType::TableStatus),
            "SHOW TABLE STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::Columns),
            "SHOW COLUMNS",
        ),
        (
            show(tidb_ast::ShowStmtType::Warnings),
            "SHOW WARNINGS",
        ),
        (
            show(tidb_ast::ShowStmtType::Charset),
            "SHOW CHARSET",
        ),
        (
            show(tidb_ast::ShowStmtType::Variables),
            "SHOW VARIABLES",
        ),
        (
            show(tidb_ast::ShowStmtType::Status),
            "SHOW STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::Collation),
            "SHOW COLLATION",
        ),
        (
            show(tidb_ast::ShowStmtType::CreateTable),
            "SHOW CREATE TABLE",
        ),
        (show(tidb_ast::ShowStmtType::CreateView), "SHOW CREATE VIEW"),
        ({
                let mut show = tidb_ast::ShowStmt::new(tidb_ast::ShowStmtType::CreateUser);
                show.user = Some(user());
                AdminStmt::Show(Box::new(show))
            }, "SHOW CREATE USER"),
        (
            show(tidb_ast::ShowStmtType::CreateSequence),
            "SHOW CREATE SEQUENCE",
        ),
        (
            show(tidb_ast::ShowStmtType::CreatePlacementPolicy),
            "SHOW CREATE PLACEMENT POLICY",
        ),
        (
            show(tidb_ast::ShowStmtType::Grants),
            "SHOW GRANTS",
        ),
        (
            show(tidb_ast::ShowStmtType::MaskingPolicies),
            "SHOW MASKING POLICIES",
        ),
        (
            show(tidb_ast::ShowStmtType::Triggers),
            "SHOW TRIGGERS",
        ),
        (
            show(tidb_ast::ShowStmtType::ProcedureStatus),
            "SHOW PROCEDURE STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::FunctionStatus),
            "SHOW FUNCTION STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::Index),
            "SHOW INDEX",
        ),
        (
            show(tidb_ast::ShowStmtType::ProcessList),
            "SHOW PROCESSLIST",
        ),
        (
            show(tidb_ast::ShowStmtType::CreateDatabase),
            "SHOW CREATE DATABASE",
        ),
        (
            show(tidb_ast::ShowStmtType::Config),
            "SHOW CONFIG",
        ),
        (
            show(tidb_ast::ShowStmtType::Events),
            "SHOW EVENTS",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsExtended),
            "SHOW STATS_EXTENDED",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsMeta),
            "SHOW STATS_META",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsHistograms),
            "SHOW STATS_HISTOGRAMS",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsTopN),
            "SHOW STATS_TOPN",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsBuckets),
            "SHOW STATS_BUCKETS",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsHealthy),
            "SHOW STATS_HEALTHY",
        ),
        (
            show(tidb_ast::ShowStmtType::StatsLocked),
            "SHOW STATS_LOCKED",
        ),
        (
            show(tidb_ast::ShowStmtType::HistogramsInFlight),
            "SHOW HISTOGRAMS_IN_FLIGHT",
        ),
        (
            show(tidb_ast::ShowStmtType::ColumnStatsUsage),
            "SHOW COLUMN_STATS_USAGE",
        ),
        (
            show(tidb_ast::ShowStmtType::Plugins),
            "SHOW PLUGINS",
        ),
        (
            show(tidb_ast::ShowStmtType::Profile),
            "SHOW PROFILE",
        ),
        (
            show(tidb_ast::ShowStmtType::Profiles),
            "SHOW PROFILES",
        ),
        (show(tidb_ast::ShowStmtType::MasterStatus), "SHOW MASTER STATUS"),
        (show(tidb_ast::ShowStmtType::Privileges), "SHOW PRIVILEGES"),
        (
            show(tidb_ast::ShowStmtType::Errors),
            "SHOW ERRORS",
        ),
        (
            show(tidb_ast::ShowStmtType::Bindings),
            "SHOW BINDINGS",
        ),
        (
            show(tidb_ast::ShowStmtType::BindingCacheStatus),
            "SHOW BINDING_CACHE STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::OpenTables),
            "SHOW OPEN TABLES",
        ),
        (
            show(tidb_ast::ShowStmtType::AnalyzeStatus),
            "SHOW ANALYZE STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::Regions),
            "SHOW TABLE REGIONS",
        ),
        (show(tidb_ast::ShowStmtType::Builtins), "SHOW BUILTINS"),
        (
            show(tidb_ast::ShowStmtType::TableNextRowId),
            "SHOW TABLE NEXT_ROW_ID",
        ),
        (
            show(tidb_ast::ShowStmtType::Backups),
            "SHOW BACKUPS",
        ),
        (
            show(tidb_ast::ShowStmtType::Restores),
            "SHOW RESTORES",
        ),
        (
            show(tidb_ast::ShowStmtType::Imports),
            "SHOW IMPORTS",
        ),
        (
            show(tidb_ast::ShowStmtType::Placement),
            "SHOW PLACEMENT",
        ),
        (
            show(tidb_ast::ShowStmtType::PlacementForDatabase),
            "SHOW PLACEMENT FOR DATABASE",
        ),
        (
            show(tidb_ast::ShowStmtType::PlacementForTable),
            "SHOW PLACEMENT FOR TABLE",
        ),
        (
            show(tidb_ast::ShowStmtType::PlacementForPartition),
            "SHOW PLACEMENT FOR PARTITION",
        ),
        (
            show(tidb_ast::ShowStmtType::PlacementLabels),
            "SHOW PLACEMENT LABELS",
        ),
        (
            show(tidb_ast::ShowStmtType::SessionStates),
            "SHOW SESSION_STATES",
        ),
        (
            show(tidb_ast::ShowStmtType::CreateResourceGroup),
            "SHOW CREATE RESOURCE GROUP",
        ),
        (
            show(tidb_ast::ShowStmtType::ImportJobs),
            "SHOW IMPORT JOBS",
        ),
        (
            show(tidb_ast::ShowStmtType::ImportGroups),
            "SHOW IMPORT GROUPS",
        ),
        (
            show(tidb_ast::ShowStmtType::CreateProcedure),
            "SHOW CREATE PROCEDURE",
        ),
        (
            show(tidb_ast::ShowStmtType::BinlogStatus),
            "SHOW BINARY LOG STATUS",
        ),
        (
            show(tidb_ast::ShowStmtType::ReplicaStatus),
            "SHOW",
        ),
        (
            show(tidb_ast::ShowStmtType::Distributions),
            "SHOW DISTRIBUTIONS",
        ),
        (
            show(tidb_ast::ShowStmtType::DistributionJobs),
            "SHOW DISTRIBUTION JOB",
        ),
        (
            show(tidb_ast::ShowStmtType::Affinity),
            "SHOW AFFINITY",
        ),
    ];

    for (stmt, expected) in cases {
        let command = admin(stmt).sem_command();
        assert_ne!(
            command,
            unknown_command(),
            "SEMCommand should not be UnknownCommand for {expected}"
        );
        assert_eq!(command, expected);
    }
}

/// `pkg/parser/ast/sem_test.go::TestShowCommand`'s `ShowCreateImport` row.
#[test]
fn show_command_create_import() {
    assert_eq!(
        admin(show(tidb_ast::ShowStmtType::CreateImport)).sem_command(),
        "SHOW CREATE IMPORT"
    );
}

/// `pkg/parser/ast/sem_test.go::TestAdminCommand`.
///
/// Go iterates `AdminStmtType` from 1 to `adminTpCount-1` on a zero
/// `AdminStmt` and requires a non-UNKNOWN SEM command. Rust folds
/// `AdminShowDDLJobQueriesWithRange` into `ShowDdlJobQueries` and
/// `AdminCheckIndexRange` into `AdminCheckStmt::Index` with ranges.
#[test]
fn admin_command() {
    let table = vec!["t".to_string()];
    let cases: Vec<(AdminStmt, &str)> = vec![
        (AdminStmt::ShowDdl, "ADMIN SHOW DDL"),
        (
            AdminStmt::AdminCheck(Box::new(AdminCheckStmt::Table {
                tables: vec![table.clone()],
            })),
            "ADMIN CHECK TABLE",
        ),
        (
            AdminStmt::ShowDdlJobs(Box::new(AdminShowDdlJobsStmt {
                job_number: 0,
                where_clause: None,
            })),
            "ADMIN SHOW DDL JOBS",
        ),
        (
            AdminStmt::DdlJobControl(Box::new(AdminDdlJobControlStmt {
                kind: AdminDdlJobControlKind::Cancel,
                job_ids: vec![1],
            })),
            "ADMIN CANCEL DDL JOBS",
        ),
        (
            AdminStmt::DdlJobControl(Box::new(AdminDdlJobControlStmt {
                kind: AdminDdlJobControlKind::Pause,
                job_ids: vec![1],
            })),
            "ADMIN PAUSE DDL JOBS",
        ),
        (
            AdminStmt::DdlJobControl(Box::new(AdminDdlJobControlStmt {
                kind: AdminDdlJobControlKind::Resume,
                job_ids: vec![1],
            })),
            "ADMIN RESUME DDL JOBS",
        ),
        (
            AdminStmt::AdminCheck(Box::new(AdminCheckStmt::Index {
                table: table.clone(),
                index: "i".to_string(),
                handle_ranges: Vec::new(),
            })),
            "ADMIN CHECK INDEX",
        ),
        (
            AdminStmt::AdminRecoverIndex(Box::new(AdminRecoverIndexStmt {
                table: table.clone(),
                index: "i".to_string(),
            })),
            "ADMIN RECOVER INDEX",
        ),
        (
            AdminStmt::AdminCleanupIndex(Box::new(AdminRecoverIndexStmt {
                table: table.clone(),
                index: "i".to_string(),
            })),
            "ADMIN CLEANUP INDEX",
        ),
        (
            AdminStmt::AdminCheck(Box::new(AdminCheckStmt::Index {
                table: table.clone(),
                index: "i".to_string(),
                handle_ranges: vec![AdminCheckHandleRange { begin: 1, end: 2 }],
            })),
            "ADMIN CHECK INDEX RANGE",
        ),
        (
            AdminStmt::ShowDdlJobQueries(Box::new(AdminShowDdlJobQueriesStmt::JobIds(vec![1]))),
            "ADMIN SHOW DDL JOB QUERIES",
        ),
        (
            AdminStmt::ShowDdlJobQueries(Box::new(AdminShowDdlJobQueriesStmt::Limit {
                offset: 0,
                count: 1,
            })),
            "ADMIN SHOW DDL JOB QUERIES",
        ),
        (
            AdminStmt::AdminChecksum(Box::new(AdminChecksumStmt {
                tables: vec![table.clone()],
            })),
            "ADMIN CHECKSUM TABLE",
        ),
        (
            AdminStmt::ShowSlow(Box::new(AdminShowSlowStmt {
                mode: AdminShowSlowMode::Recent,
                count: 1,
            })),
            "ADMIN SHOW SLOW",
        ),
        (
            AdminStmt::ShowNextRowId(Box::new(AdminShowNextRowIdStmt { table })),
            "ADMIN SHOW NEXT_ROW_ID",
        ),
        (
            AdminStmt::Reload(AdminReloadKind::ExprPushdownBlacklist),
            "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST",
        ),
        (
            AdminStmt::Reload(AdminReloadKind::OptRuleBlacklist),
            "ADMIN RELOAD OPT_RULE_BLACKLIST",
        ),
        (
            AdminStmt::Plugins {
                enable: false,
                plugins: vec!["p".to_string()],
            },
            "ADMIN PLUGINS DISABLE",
        ),
        (
            AdminStmt::Plugins {
                enable: true,
                plugins: vec!["p".to_string()],
            },
            "ADMIN PLUGINS ENABLE",
        ),
        (
            AdminStmt::BindingControl(AdminBindingControlKind::Flush),
            "ADMIN FLUSH BINDINGS",
        ),
        (
            AdminStmt::BindingControl(AdminBindingControlKind::Capture),
            "ADMIN CAPTURE BINDINGS",
        ),
        (
            AdminStmt::BindingControl(AdminBindingControlKind::Evolve),
            "ADMIN EVOLVE BINDINGS",
        ),
        (
            AdminStmt::Reload(AdminReloadKind::Bindings),
            "ADMIN RELOAD BINDINGS",
        ),
        (
            AdminStmt::Reload(AdminReloadKind::Statistics),
            "ADMIN RELOAD STATS_EXTENDED",
        ),
        (
            AdminStmt::FlushPlanCache(AdminPlanCacheScope::Session),
            "ADMIN FLUSH PLAN_CACHE",
        ),
        (
            AdminStmt::SetBdrRole(BdrRole::Primary),
            "ADMIN SET BDR ROLE",
        ),
        (AdminStmt::ShowBdrRole, "ADMIN SHOW BDR ROLE"),
        (AdminStmt::UnsetBdrRole, "ADMIN UNSET BDR ROLE"),
        (
            AdminStmt::AlterDdlJobs(Box::new(AdminAlterDdlJobsStmt {
                job_number: 1,
                options: Vec::new(),
            })),
            "ADMIN ALTER DDL JOBS",
        ),
        (
            AdminStmt::CreateWorkloadSnapshot,
            "ADMIN CREATE WORKLOAD SNAPSHOT",
        ),
        (
            AdminStmt::Reload(AdminReloadKind::ClusterBindings),
            "ADMIN RELOAD CLUSTER BINDINGS",
        ),
    ];

    for (stmt, expected) in cases {
        let command = admin(stmt).sem_command();
        assert_ne!(
            command,
            unknown_command(),
            "SEMCommand should not be UnknownCommand for {expected}"
        );
        assert_eq!(command, expected);
    }
}

/// `pkg/parser/ast/sem_test.go::TestBRIECommand`.
///
/// Go iterates `for i := range brieKindCount` on a zero `BRIEStmt`. Rust's
/// closed `BrieKind` plus the exhaustive SEM match already reject an omitted
/// kind at compile time; the table pins every source command string.
#[test]
fn brie_command() {
    let cases = [
        (BrieKind::Backup, "BACKUP"),
        (BrieKind::Restore, "RESTORE"),
        (BrieKind::RestorePoint, "RESTORE POINT"),
        (BrieKind::StreamStart, "BACKUP LOGS"),
        (BrieKind::StreamStop, "STOP BACKUP LOGS"),
        (BrieKind::StreamPause, "PAUSE BACKUP LOGS"),
        (BrieKind::StreamResume, "RESUME BACKUP LOGS"),
        (BrieKind::StreamStatus, "SHOW BACKUP LOGS STATUS"),
        (BrieKind::StreamMetadata, "SHOW BACKUP LOGS METADATA"),
        (BrieKind::StreamPurge, "PURGE BACKUP LOGS"),
        (BrieKind::ShowJob, "SHOW BR JOB"),
        (BrieKind::ShowQuery, "SHOW BR JOB QUERY"),
        (BrieKind::CancelJob, "CANCEL BR JOB"),
        (BrieKind::ShowBackupMetadata, "SHOW BACKUP META"),
    ];
    for (kind, expected) in cases {
        let stmt = admin(AdminStmt::Brie(Box::new(BrieStmt {
            kind,
            schemas: Vec::new(),
            tables: Vec::new(),
            storage: String::new(),
            job_id: 0,
            options: Vec::new(),
        })));
        let command = stmt.sem_command();
        assert_ne!(
            command,
            unknown_command(),
            "SEMCommand should not be UnknownCommand for BRIEKind {expected}"
        );
        assert_eq!(command, expected);
    }
}
