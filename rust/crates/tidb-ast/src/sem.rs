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

//! Security-enhanced-mode command classification transcreated from
//! `pkg/parser/ast/sem.go`.

use crate::{
    AdminBindingControlKind, AdminCheckStmt, AdminDdlJobControlKind, AdminReloadKind, AdminStmt,
    BrieKind, DdlStmt, DmlStmt, QueryStmt, ServerControlStmt, SessionStmt,
    Stmt,
};

impl Stmt {
    /// Returns TiDB's stable SEM command string for this statement.
    pub fn sem_command(&self) -> &'static str {
        match self {
            Self::Query(query) => query.sem_command(),
            Self::Dml(dml) => dml.sem_command(),
            Self::Ddl(ddl) => ddl.sem_command(),
            Self::Admin(admin) => admin.sem_command(),
            Self::Session(session) => session.sem_command(),
        }
    }
}

impl QueryStmt {
    fn sem_command(&self) -> &'static str {
        match self {
            Self::Select(_) => "SELECT",
            Self::SetOpr(_) => "SET OPERATION",
        }
    }
}

impl DmlStmt {
    fn sem_command(&self) -> &'static str {
        match self {
            Self::Insert(insert) if insert.replace => "REPLACE",
            Self::Insert(_) => "INSERT",
            Self::Update(_) => "UPDATE",
            Self::Delete(_) => "DELETE",
            Self::ImportInto(_) => "IMPORT INTO",
            Self::LoadData(_) => "LOAD DATA",
            Self::Batch(_) => "BATCH",
            Self::DistributeTable(_) => "DISTRIBUTE TABLE",
            Self::Call(_) => "CALL",
        }
    }
}

impl DdlStmt {
    fn sem_command(&self) -> &'static str {
        match self {
            Self::CreateTable(_) => "CREATE TABLE",
            Self::CreateView(_) => "CREATE VIEW",
            Self::AlterView(_) => "ALTER VIEW",
            Self::CreateIndex(_) => "CREATE INDEX",
            Self::DropIndex(_) => "DROP INDEX",
            Self::CreateDatabase { .. } => "CREATE DATABASE",
            Self::AlterDatabase { .. } => "ALTER DATABASE",
            Self::CreatePlacementPolicy(_) => "CREATE PLACEMENT POLICY",
            Self::AlterPlacementPolicy(_) => "ALTER PLACEMENT POLICY",
            Self::AlterTable(_) => "ALTER TABLE",
            Self::RenameTable(_) => "RENAME TABLE",
            Self::RenameUser { .. } => "RENAME USER",
            Self::LockTables(_) => "LOCK TABLES",
            Self::UnlockTables => "UNLOCK TABLES",
            Self::DropTable(_) => "DROP TABLE",
            Self::DropView { .. } => "DROP VIEW",
            Self::DropDatabase { .. } => "DROP DATABASE",
            Self::DropPlacementPolicy(_) => "DROP PLACEMENT POLICY",
            Self::DropResourceGroup(_) => "DROP RESOURCE GROUP",
            Self::CreateResourceGroup(_) => "CREATE RESOURCE GROUP",
            Self::AlterResourceGroup(_) => "ALTER RESOURCE GROUP",
            Self::CreateMaskingPolicy(_) => "CREATE MASKING POLICY",
            Self::CreateUser { .. } | Self::CreateRole { .. } => "CREATE USER",
            Self::AlterUser(_) => "ALTER USER",
            Self::DropUser { .. } => "DROP USER",
            Self::TruncateTable(_) => "TRUNCATE TABLE",
            Self::CreateSequence(_) => "CREATE SEQUENCE",
            Self::AlterSequence(_) => "ALTER SEQUENCE",
            Self::DropSequence(_) => "DROP SEQUENCE",
            Self::CreateProcedure(_) | Self::DropProcedure(_) => "PROCEDURE",
            Self::AlterInstance(_) => "ALTER INSTANCE",
            Self::AlterRange(_) => "ALTER RANGE",
            Self::FlashbackDatabase(_) => "FLASHBACK DATABASE",
            Self::RecoverTable(_) => "RECOVER TABLE",
            Self::FlashbackToTimestamp(_) => "FLASHBACK CLUSTER",
            Self::FlashbackTable(_) => "FLASHBACK TABLE",
            Self::OptimizeTable(_) => "OPTIMIZE TABLE",
            Self::RepairTable(_) => "ADMIN REPAIR TABLE",
        }
    }
}

impl SessionStmt {
    fn sem_command(&self) -> &'static str {
        match self {
            Self::Use(_) => "USE",
            Self::Set(_) | Self::SetUserVar(_) | Self::SetCharset { .. } | Self::SetMixed(_) => {
                "SET"
            }
            Self::SetPassword(_) => "SET PASSWORD",
            Self::SetRole(_) => "SET ROLE",
            Self::SetDefaultRole(_) => "SET DEFAULT ROLE",
            Self::SetResourceGroup(_) => "SET RESOURCE GROUP",
            Self::SetSessionStates(_) => "SET SESSION_STATES",
            Self::Prepare { .. } => "PREPARE",
            Self::Execute { .. } => "EXECUTE",
            Self::Deallocate(_) => "DEALLOCATE",
            Self::Begin(_) => "BEGIN",
            Self::Commit(_) => "COMMIT",
            Self::Rollback { .. } => "ROLLBACK",
            Self::Savepoint(_) => "SAVEPOINT",
            Self::ReleaseSavepoint(_) => "RELEASE SAVEPOINT",
        }
    }
}

impl crate::ShowStmtType {
    /// The command name SEM's restricted-SQL list matches a SHOW against.
    fn sem_command(self) -> &'static str {
        use crate::ShowStmtType;
        match self {
            ShowStmtType::Grants => "SHOW GRANTS",
            ShowStmtType::MasterStatus => "SHOW MASTER STATUS",
            ShowStmtType::Privileges => "SHOW PRIVILEGES",
            ShowStmtType::Builtins => "SHOW BUILTINS",
            ShowStmtType::ImportJobs => "SHOW IMPORT JOBS",
            ShowStmtType::ImportGroups => "SHOW IMPORT GROUPS",
            ShowStmtType::TableNextRowId => "SHOW TABLE NEXT_ROW_ID",
            ShowStmtType::CreateTable => "SHOW CREATE TABLE",
            ShowStmtType::CreateView => "SHOW CREATE VIEW",
            ShowStmtType::CreateSequence => "SHOW CREATE SEQUENCE",
            ShowStmtType::CreateDatabase => "SHOW CREATE DATABASE",
            ShowStmtType::CreateProcedure => "SHOW CREATE PROCEDURE",
            ShowStmtType::CreatePlacementPolicy => "SHOW CREATE PLACEMENT POLICY",
            ShowStmtType::CreateResourceGroup => "SHOW CREATE RESOURCE GROUP",
            ShowStmtType::CreateImport => "SHOW CREATE IMPORT",
            ShowStmtType::CreateUser => "SHOW CREATE USER",
            ShowStmtType::Variables => "SHOW VARIABLES",
            ShowStmtType::Status => "SHOW STATUS",
            ShowStmtType::Warnings => "SHOW WARNINGS",
            ShowStmtType::Errors => "SHOW ERRORS",
            ShowStmtType::Collation => "SHOW COLLATION",
            ShowStmtType::Engines => "SHOW ENGINES",
            ShowStmtType::Charset => "SHOW CHARSET",
            ShowStmtType::StatsHistograms => "SHOW STATS_HISTOGRAMS",
            ShowStmtType::StatsBuckets => "SHOW STATS_BUCKETS",
            ShowStmtType::StatsLocked => "SHOW STATS_LOCKED",
            ShowStmtType::StatsTopN => "SHOW STATS_TOPN",
            ShowStmtType::Databases => "SHOW DATABASES",
            ShowStmtType::Tables => "SHOW TABLE",
            ShowStmtType::OpenTables => "SHOW OPEN TABLES",
            ShowStmtType::TableStatus => "SHOW TABLE STATUS",
            ShowStmtType::Columns => "SHOW COLUMNS",
            ShowStmtType::Index => "SHOW INDEX",
            ShowStmtType::Triggers => "SHOW TRIGGERS",
            ShowStmtType::ProcedureStatus => "SHOW PROCEDURE STATUS",
            ShowStmtType::FunctionStatus => "SHOW FUNCTION STATUS",
            ShowStmtType::Events => "SHOW EVENTS",
            ShowStmtType::Plugins => "SHOW PLUGINS",
            ShowStmtType::StatsExtended => "SHOW STATS_EXTENDED",
            ShowStmtType::StatsMeta => "SHOW STATS_META",
            ShowStmtType::StatsHealthy => "SHOW STATS_HEALTHY",
            ShowStmtType::HistogramsInFlight => "SHOW HISTOGRAMS_IN_FLIGHT",
            ShowStmtType::ColumnStatsUsage => "SHOW COLUMN_STATS_USAGE",
            ShowStmtType::BindingCacheStatus => "SHOW BINDING_CACHE STATUS",
            ShowStmtType::AnalyzeStatus => "SHOW ANALYZE STATUS",
            ShowStmtType::Backups => "SHOW BACKUPS",
            ShowStmtType::Restores => "SHOW RESTORES",
            ShowStmtType::Imports => "SHOW IMPORTS",
            ShowStmtType::Config => "SHOW CONFIG",
            ShowStmtType::ReplicaStatus => "SHOW",
            ShowStmtType::BinlogStatus => "SHOW BINARY LOG STATUS",
            ShowStmtType::Profiles => "SHOW PROFILES",
            ShowStmtType::SessionStates => "SHOW SESSION_STATES",
            ShowStmtType::ProcessList => "SHOW PROCESSLIST",
            ShowStmtType::Affinity => "SHOW AFFINITY",
            ShowStmtType::DistributionJobs => "SHOW DISTRIBUTION JOB",
            ShowStmtType::Regions => "SHOW TABLE REGIONS",
            ShowStmtType::Distributions => "SHOW DISTRIBUTIONS",
            ShowStmtType::Placement => "SHOW PLACEMENT",
            ShowStmtType::PlacementForDatabase => "SHOW PLACEMENT FOR DATABASE",
            ShowStmtType::PlacementForTable => "SHOW PLACEMENT FOR TABLE",
            ShowStmtType::PlacementForPartition => "SHOW PLACEMENT FOR PARTITION",
            ShowStmtType::PlacementLabels => "SHOW PLACEMENT LABELS",
            ShowStmtType::Profile => "SHOW PROFILE",
            ShowStmtType::MaskingPolicies => "SHOW MASKING POLICIES",
            ShowStmtType::Bindings => "SHOW BINDINGS",
            ShowStmtType::None => "SHOW",
        }
    }
}

impl AdminStmt {
    fn sem_command(&self) -> &'static str {
        match self {
            Self::Show(show) => show.tp.sem_command(),
            Self::Grant(_) => "GRANT",
            Self::GrantProxy(_) => "GRANT PROXY",
            Self::GrantRole(_) => "GRANT ROLE",
            Self::Revoke(_) => "REVOKE",
            Self::RevokeRole(_) => "REVOKE ROLE",
            Self::Brie(brie) => brie.kind.sem_command(),
            Self::Trace(_) => "TRACE",
            Self::ExplainFor(_) => "EXPLAIN FOR CONNECTION",
            Self::Binlog(_) => "BINLOG",
            Self::Kill(_) => "KILL",
            Self::SetConfig(_) => "SET CONFIG",
            Self::RecommendIndex(_) => "RECOMMEND INDEX",
            Self::CreateStatistics(_) => "CREATE STATISTICS",
            Self::DropStatistics(_) => "DROP STATISTICS",
            Self::ServerControl(control) => match control.as_ref() {
                ServerControlStmt::Shutdown => "SHUTDOWN",
                ServerControlStmt::Restart => "RESTART",
                ServerControlStmt::Help(_) => "HELP",
            },
            Self::CancelDistributionJob(_) => "CANCEL DISTRIBUTION JOB",
            Self::CalibrateResource(_) => "CALIBRATE RESOURCE",
            Self::AddQueryWatch(_) => "ADD QUERY WATCH",
            Self::DropQueryWatch(_) => "DROP QUERY WATCH",
            Self::CancelImportJob(_) => "CANCEL IMPORT INTO JOB",
            Self::Flush(_) => "FLUSH",
            Self::FlushPlanCache(_) => "ADMIN FLUSH PLAN_CACHE",
            Self::Do(_) => "DO",
            Self::Reload(kind) => match kind {
                AdminReloadKind::Statistics => "ADMIN RELOAD STATS_EXTENDED",
                AdminReloadKind::OptRuleBlacklist => "ADMIN RELOAD OPT_RULE_BLACKLIST",
                AdminReloadKind::ExprPushdownBlacklist => "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST",
                AdminReloadKind::Bindings => "ADMIN RELOAD BINDINGS",
                AdminReloadKind::ClusterBindings => "ADMIN RELOAD CLUSTER BINDINGS",
            },
            Self::SetBdrRole(_) => "ADMIN SET BDR ROLE",
            Self::UnsetBdrRole => "ADMIN UNSET BDR ROLE",
            Self::ShowBdrRole => "ADMIN SHOW BDR ROLE",
            Self::ShowSlow(_) => "ADMIN SHOW SLOW",
            Self::ShowDdl => "ADMIN SHOW DDL",
            Self::ShowDdlJobs(_) => "ADMIN SHOW DDL JOBS",
            Self::ShowDdlJobQueries(_) => "ADMIN SHOW DDL JOB QUERIES",
            Self::DdlJobControl(control) => match control.kind {
                AdminDdlJobControlKind::Cancel => "ADMIN CANCEL DDL JOBS",
                AdminDdlJobControlKind::Pause => "ADMIN PAUSE DDL JOBS",
                AdminDdlJobControlKind::Resume => "ADMIN RESUME DDL JOBS",
            },
            Self::AlterDdlJobs(_) => "ADMIN ALTER DDL JOBS",
            Self::ShowNextRowId(_) => "ADMIN SHOW NEXT_ROW_ID",
            Self::CreateBinding(_) => "CREATE BINDING",
            Self::DropBinding(_) => "DROP BINDING",
            Self::SetBinding(_) => "SET BINDING",
            Self::AnalyzeTable(_) | Self::AnalyzeIncremental(_) => "ANALYZE TABLE",
            Self::Traffic(_) => "TRAFFIC",
            Self::RefreshStats(_) => "REFRESH STATS",
            Self::AdminCheck(check) => match check.as_ref() {
                AdminCheckStmt::Table { .. } => "ADMIN CHECK TABLE",
                AdminCheckStmt::Index { handle_ranges, .. } if !handle_ranges.is_empty() => {
                    "ADMIN CHECK INDEX RANGE"
                }
                AdminCheckStmt::Index { .. } => "ADMIN CHECK INDEX",
            },
            Self::AdminChecksum(_) => "ADMIN CHECKSUM TABLE",
            Self::AdminRecoverIndex(_) => "ADMIN RECOVER INDEX",
            Self::AdminCleanupIndex(_) => "ADMIN CLEANUP INDEX",
            Self::Plugins { enable, .. } => {
                if *enable {
                    "ADMIN PLUGINS ENABLE"
                } else {
                    "ADMIN PLUGINS DISABLE"
                }
            }
            Self::BindingControl(kind) => match kind {
                AdminBindingControlKind::Flush => "ADMIN FLUSH BINDINGS",
                AdminBindingControlKind::Capture => "ADMIN CAPTURE BINDINGS",
                AdminBindingControlKind::Evolve => "ADMIN EVOLVE BINDINGS",
            },
            Self::CreateWorkloadSnapshot => "ADMIN CREATE WORKLOAD SNAPSHOT",
            Self::CleanupTableLock(_) => "ADMIN CLEANUP TABLE LOCK",
            Self::LockStats(_) => "LOCK STATS",
            Self::UnlockStats(_) => "UNLOCK STATS",
            Self::Explain(explain) => {
                if explain.analyze {
                    "EXPLAIN ANALYZE"
                } else {
                    "EXPLAIN"
                }
            }
            Self::PlanReplayer(_) => "PLAN REPLAYER",
            Self::LoadStats(_) => "LOAD STATS",
            Self::DropStats(_) => "DROP STATS",
            Self::SplitRegion(_) => "SPLIT REGION",
        }
    }
}

impl BrieKind {
    fn sem_command(self) -> &'static str {
        match self {
            Self::Backup => "BACKUP",
            Self::CancelJob => "CANCEL BR JOB",
            Self::StreamStart => "BACKUP LOGS",
            Self::StreamMetadata => "SHOW BACKUP LOGS METADATA",
            Self::StreamStatus => "SHOW BACKUP LOGS STATUS",
            Self::StreamPause => "PAUSE BACKUP LOGS",
            Self::StreamResume => "RESUME BACKUP LOGS",
            Self::StreamStop => "STOP BACKUP LOGS",
            Self::StreamPurge => "PURGE BACKUP LOGS",
            Self::Restore => "RESTORE",
            Self::RestorePoint => "RESTORE POINT",
            Self::ShowJob => "SHOW BR JOB",
            Self::ShowQuery => "SHOW BR JOB QUERY",
            Self::ShowBackupMetadata => "SHOW BACKUP META",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdminCheckHandleRange;

    /// Transcreates `TestShowCommand` from `pkg/parser/ast/sem_test.go`.
    ///
    /// Go's integer `ShowStmtType` can reach an unhandled default. Rust's
    /// `AdminStmt` and nested SHOW enums are closed, and the matches above are
    /// exhaustive, so adding a SHOW kind without a command fails compilation.
    #[test]
    fn show_command_is_total_over_the_closed_ast() {
        let show = |tp| AdminStmt::Show(Box::new(crate::ShowStmt::new(tp)));
        assert_eq!(
            show(crate::ShowStmtType::MasterStatus).sem_command(),
            "SHOW MASTER STATUS"
        );
        assert_eq!(show(crate::ShowStmtType::Privileges).sem_command(), "SHOW PRIVILEGES");
        assert_eq!(show(crate::ShowStmtType::Builtins).sem_command(), "SHOW BUILTINS");
    }

    /// Transcreates `TestAdminCommand` from `pkg/parser/ast/sem_test.go`.
    /// The range case is intentionally separate: Go assigns it a distinct
    /// `AdminStmtType` even though Rust removes that invalid duplicated state.
    #[test]
    fn admin_command_is_total_and_preserves_index_range_identity() {
        let table = vec!["t".to_string()];
        let index = "i".to_string();
        let plain = AdminStmt::AdminCheck(Box::new(AdminCheckStmt::Index {
            table: table.clone(),
            index: index.clone(),
            handle_ranges: Vec::new(),
        }));
        let ranged = AdminStmt::AdminCheck(Box::new(AdminCheckStmt::Index {
            table,
            index,
            handle_ranges: vec![AdminCheckHandleRange { begin: 1, end: 2 }],
        }));

        assert_eq!(AdminStmt::ShowDdl.sem_command(), "ADMIN SHOW DDL");
        assert_eq!(plain.sem_command(), "ADMIN CHECK INDEX");
        assert_eq!(ranged.sem_command(), "ADMIN CHECK INDEX RANGE");
    }

    /// Transcreates `TestBRIECommand` from `pkg/parser/ast/sem_test.go`.
    ///
    /// Go needs a runtime loop because `BRIEKind` is an open integer domain.
    /// Rust's closed enum plus the exhaustive match in `AdminStmt::sem_command`
    /// makes an omitted kind a compile error; this table also checks the exact
    /// source command for every kind.
    #[test]
    fn brie_command_covers_every_source_kind() {
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
            assert_eq!(kind.sem_command(), expected);
        }
    }
}
