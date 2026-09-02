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
    AdminShowNextRowIdStmt, AdminShowSlowMode, AdminShowSlowStmt, AdminStmt, BdrRole, BindingScope,
    BrieKind, BrieStmt, NodeBox, ShowBindingsStmt, ShowCharsetStmt, ShowCollationStmt,
    ShowColumnsStmt, ShowCreateKind, ShowDatabasesStmt, ShowDistributionJobsStmt, ShowEnginesStmt,
    ShowErrorsStmt, ShowGrantsStmt, ShowImportGroupsStmt, ShowImportJobsStmt, ShowIndexStmt,
    ShowInspectionKind, ShowInspectionStmt, ShowMaskingPoliciesStmt, ShowOpenTablesStmt,
    ShowPlacementStmt, ShowPlacementTarget, ShowProfileStmt, ShowStatsBucketsStmt,
    ShowStatsHistogramsStmt, ShowStatsLockedStmt, ShowStatsTopNStmt, ShowStatusStmt,
    ShowTableNextRowIdStmt, ShowTablePlacementKind, ShowTablePlacementStmt, ShowTableStatusStmt,
    ShowTablesStmt, ShowVariablesStmt, ShowWarningsStmt, Stmt, UserSpec,
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

fn empty_inspection(kind: ShowInspectionKind) -> ShowInspectionStmt {
    ShowInspectionStmt {
        kind,
        full: false,
        database: None,
        filter: None,
    }
}

fn empty_show_create(kind: ShowCreateKind) -> AdminStmt {
    AdminStmt::ShowCreate {
        kind,
        if_not_exists: false,
        name: vec!["t".to_string()],
    }
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
            AdminStmt::ShowEngines(Box::new(ShowEnginesStmt { filter: None })),
            "SHOW ENGINES",
        ),
        (
            AdminStmt::ShowDatabases(Box::new(ShowDatabasesStmt { filter: None })),
            "SHOW DATABASES",
        ),
        (
            AdminStmt::ShowTables(Box::new(ShowTablesStmt {
                full: false,
                database: None,
                filter: None,
            })),
            "SHOW TABLE",
        ),
        (
            AdminStmt::ShowTableStatus(Box::new(ShowTableStatusStmt {
                database: None,
                filter: None,
            })),
            "SHOW TABLE STATUS",
        ),
        (
            AdminStmt::ShowColumns(Box::new(ShowColumnsStmt {
                full: false,
                extended: false,
                table: vec!["t".to_string()],
                database: None,
                filter: None,
            })),
            "SHOW COLUMNS",
        ),
        (
            AdminStmt::ShowWarnings(Box::new(ShowWarningsStmt {
                count_only: false,
                filter: None,
            })),
            "SHOW WARNINGS",
        ),
        (
            AdminStmt::ShowCharset(Box::new(ShowCharsetStmt { filter: None })),
            "SHOW CHARSET",
        ),
        (
            AdminStmt::ShowVariables(Box::new(ShowVariablesStmt {
                global: false,
                like: None,
                where_clause: None,
            })),
            "SHOW VARIABLES",
        ),
        (
            AdminStmt::ShowStatus(Box::new(ShowStatusStmt {
                global: false,
                filter: None,
            })),
            "SHOW STATUS",
        ),
        (
            AdminStmt::ShowCollation(Box::new(ShowCollationStmt { filter: None })),
            "SHOW COLLATION",
        ),
        (
            empty_show_create(ShowCreateKind::Table),
            "SHOW CREATE TABLE",
        ),
        (empty_show_create(ShowCreateKind::View), "SHOW CREATE VIEW"),
        (AdminStmt::ShowCreateUser(user()), "SHOW CREATE USER"),
        (
            empty_show_create(ShowCreateKind::Sequence),
            "SHOW CREATE SEQUENCE",
        ),
        (
            empty_show_create(ShowCreateKind::PlacementPolicy),
            "SHOW CREATE PLACEMENT POLICY",
        ),
        (
            AdminStmt::ShowGrants(Box::new(ShowGrantsStmt {
                user: None,
                roles: Vec::new(),
            })),
            "SHOW GRANTS",
        ),
        (
            AdminStmt::ShowMaskingPolicies(Box::new(ShowMaskingPoliciesStmt {
                table: vec!["t".to_string()],
                where_clause: None,
            })),
            "SHOW MASKING POLICIES",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Triggers))),
            "SHOW TRIGGERS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::ProcedureStatus,
            ))),
            "SHOW PROCEDURE STATUS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::FunctionStatus,
            ))),
            "SHOW FUNCTION STATUS",
        ),
        (
            AdminStmt::ShowIndex(Box::new(ShowIndexStmt {
                table: vec!["t".to_string()],
                filter: None,
            })),
            "SHOW INDEX",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::ProcessList))),
            "SHOW PROCESSLIST",
        ),
        (
            empty_show_create(ShowCreateKind::Database),
            "SHOW CREATE DATABASE",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Config))),
            "SHOW CONFIG",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Events))),
            "SHOW EVENTS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::StatsExtended,
            ))),
            "SHOW STATS_EXTENDED",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::StatsMeta))),
            "SHOW STATS_META",
        ),
        (
            AdminStmt::ShowStatsHistograms(Box::new(ShowStatsHistogramsStmt { filter: None })),
            "SHOW STATS_HISTOGRAMS",
        ),
        (
            AdminStmt::ShowStatsTopN(Box::new(ShowStatsTopNStmt { filter: None })),
            "SHOW STATS_TOPN",
        ),
        (
            AdminStmt::ShowStatsBuckets(Box::new(ShowStatsBucketsStmt { filter: None })),
            "SHOW STATS_BUCKETS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::StatsHealthy))),
            "SHOW STATS_HEALTHY",
        ),
        (
            AdminStmt::ShowStatsLocked(Box::new(ShowStatsLockedStmt { filter: None })),
            "SHOW STATS_LOCKED",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::HistogramsInFlight,
            ))),
            "SHOW HISTOGRAMS_IN_FLIGHT",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::ColumnStatsUsage,
            ))),
            "SHOW COLUMN_STATS_USAGE",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Plugins))),
            "SHOW PLUGINS",
        ),
        (
            AdminStmt::ShowProfile(Box::new(ShowProfileStmt {
                types: Vec::new(),
                query_id: None,
                limit: None,
            })),
            "SHOW PROFILE",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Profiles))),
            "SHOW PROFILES",
        ),
        (AdminStmt::ShowMasterStatus, "SHOW MASTER STATUS"),
        (AdminStmt::ShowPrivileges, "SHOW PRIVILEGES"),
        (
            AdminStmt::ShowErrors(Box::new(ShowErrorsStmt {
                count_only: false,
                filter: None,
            })),
            "SHOW ERRORS",
        ),
        (
            AdminStmt::ShowBindings(Box::new(ShowBindingsStmt {
                scope: BindingScope::Session,
                filter: None,
            })),
            "SHOW BINDINGS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::BindingCacheStatus,
            ))),
            "SHOW BINDING_CACHE STATUS",
        ),
        (
            AdminStmt::ShowOpenTables(Box::new(ShowOpenTablesStmt {
                database: None,
                filter: None,
            })),
            "SHOW OPEN TABLES",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::AnalyzeStatus,
            ))),
            "SHOW ANALYZE STATUS",
        ),
        (
            AdminStmt::ShowTablePlacement(Box::new(ShowTablePlacementStmt {
                table: vec!["t".to_string()],
                partitions: Vec::new(),
                index: None,
                kind: ShowTablePlacementKind::Regions,
                filter: None,
            })),
            "SHOW TABLE REGIONS",
        ),
        (AdminStmt::ShowBuiltins, "SHOW BUILTINS"),
        (
            AdminStmt::ShowTableNextRowId(Box::new(ShowTableNextRowIdStmt {
                table: vec!["t".to_string()],
            })),
            "SHOW TABLE NEXT_ROW_ID",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Backups))),
            "SHOW BACKUPS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Restores))),
            "SHOW RESTORES",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Imports))),
            "SHOW IMPORTS",
        ),
        (
            AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
                target: ShowPlacementTarget::All,
                filter: None,
            })),
            "SHOW PLACEMENT",
        ),
        (
            AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
                target: ShowPlacementTarget::Database("d".to_string()),
                filter: None,
            })),
            "SHOW PLACEMENT FOR DATABASE",
        ),
        (
            AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
                target: ShowPlacementTarget::Table(vec!["t".to_string()]),
                filter: None,
            })),
            "SHOW PLACEMENT FOR TABLE",
        ),
        (
            AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
                target: ShowPlacementTarget::Partition {
                    table: vec!["t".to_string()],
                    partition: "p0".to_string(),
                },
                filter: None,
            })),
            "SHOW PLACEMENT FOR PARTITION",
        ),
        (
            AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
                target: ShowPlacementTarget::Labels,
                filter: None,
            })),
            "SHOW PLACEMENT LABELS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::SessionStates,
            ))),
            "SHOW SESSION_STATES",
        ),
        (
            empty_show_create(ShowCreateKind::ResourceGroup),
            "SHOW CREATE RESOURCE GROUP",
        ),
        (
            AdminStmt::ShowImportJobs(Box::new(ShowImportJobsStmt {
                raw: false,
                job_id: None,
                where_clause: None,
            })),
            "SHOW IMPORT JOBS",
        ),
        (
            AdminStmt::ShowImportGroups(Box::new(ShowImportGroupsStmt {
                group_key: None,
                where_clause: None,
            })),
            "SHOW IMPORT GROUPS",
        ),
        (
            empty_show_create(ShowCreateKind::Procedure),
            "SHOW CREATE PROCEDURE",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::BinaryLogStatus,
            ))),
            "SHOW BINARY LOG STATUS",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(
                ShowInspectionKind::ReplicaStatus,
            ))),
            "SHOW",
        ),
        (
            AdminStmt::ShowTablePlacement(Box::new(ShowTablePlacementStmt {
                table: vec!["t".to_string()],
                partitions: Vec::new(),
                index: None,
                kind: ShowTablePlacementKind::Distributions,
                filter: None,
            })),
            "SHOW DISTRIBUTIONS",
        ),
        (
            AdminStmt::ShowDistributionJobs(Box::new(ShowDistributionJobsStmt {
                job_id: None,
                filter: None,
            })),
            "SHOW DISTRIBUTION JOB",
        ),
        (
            AdminStmt::ShowInspection(Box::new(empty_inspection(ShowInspectionKind::Affinity))),
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
        admin(empty_show_create(ShowCreateKind::Import)).sem_command(),
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
