// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

// DDL Statements

const (
	// AlterDatabaseCommand represents ALTER DATABASE statement
	AlterDatabaseCommand = "ALTER DATABASE"
	// AlterInstanceCommand represents ALTER INSTANCE statement
	AlterInstanceCommand = "ALTER INSTANCE"
	// AlterPlacementPolicyCommand represents ALTER PLACEMENT POLICY statement
	AlterPlacementPolicyCommand = "ALTER PLACEMENT POLICY"
	// AlterRangeCommand represents ALTER RANGE statement
	AlterRangeCommand = "ALTER RANGE"
	// AlterResourceGroupCommand represents ALTER RESOURCE GROUP statement
	AlterResourceGroupCommand = "ALTER RESOURCE GROUP"
	// AlterSequenceCommand represents ALTER SEQUENCE statement
	AlterSequenceCommand = "ALTER SEQUENCE"
	// AlterTableCommand represents ALTER TABLE statement
	AlterTableCommand = "ALTER TABLE"
	// AlterUserCommand represents ALTER USER statement
	AlterUserCommand = "ALTER USER"
	// AdminCleanupTableLockCommand represents ADMIN CLEANUP TABLE LOCK statement
	AdminCleanupTableLockCommand = "ADMIN CLEANUP TABLE LOCK"
	// CreateDatabaseCommand represents CREATE DATABASE statement
	CreateDatabaseCommand = "CREATE DATABASE"
	// CreateIndexCommand represents CREATE INDEX statement
	CreateIndexCommand = "CREATE INDEX"
	// CreatePlacementPolicyCommand represents CREATE PLACEMENT POLICY statement
	CreatePlacementPolicyCommand = "CREATE PLACEMENT POLICY"
	// CreateResourceGroupCommand represents CREATE RESOURCE GROUP statement
	CreateResourceGroupCommand = "CREATE RESOURCE GROUP"
	// CreateSequenceCommand represents CREATE SEQUENCE statement
	CreateSequenceCommand = "CREATE SEQUENCE"
	// CreateTableCommand represents CREATE TABLE statement
	CreateTableCommand = "CREATE TABLE"
	// CreateUserCommand represents CREATE USER statement
	CreateUserCommand = "CREATE USER"
	// CreateViewCommand represents CREATE VIEW statement
	CreateViewCommand = "CREATE VIEW"
	// CreateMaterializedViewLogCommand represents CREATE MATERIALIZED VIEW LOG statement
	CreateMaterializedViewLogCommand = "CREATE MATERIALIZED VIEW LOG"
	// CreateMaterializedViewCommand represents CREATE MATERIALIZED VIEW statement
	CreateMaterializedViewCommand = "CREATE MATERIALIZED VIEW"
	// DropDatabaseCommand represents DROP DATABASE statement
	DropDatabaseCommand = "DROP DATABASE"
	// DropIndexCommand represents DROP INDEX statement
	DropIndexCommand = "DROP INDEX"
	// DropPlacementPolicyCommand represents DROP PLACEMENT POLICY statement
	DropPlacementPolicyCommand = "DROP PLACEMENT POLICY"
	// DropResourceGroupCommand represents DROP RESOURCE GROUP statement
	DropResourceGroupCommand = "DROP RESOURCE GROUP"
	// DropSequenceCommand represents DROP SEQUENCE statement
	DropSequenceCommand = "DROP SEQUENCE"
	// DropTableCommand represents DROP TABLE statement
	DropTableCommand = "DROP TABLE"
	// DropViewCommand represents DROP VIEW statement
	DropViewCommand = "DROP VIEW"
	// DropMaterializedViewLogCommand represents DROP MATERIALIZED VIEW LOG statement
	DropMaterializedViewLogCommand = "DROP MATERIALIZED VIEW LOG"
	// DropMaterializedViewCommand represents DROP MATERIALIZED VIEW statement
	DropMaterializedViewCommand = "DROP MATERIALIZED VIEW"
	// DropUserCommand represents DROP USER statement
	DropUserCommand = "DROP USER"
	// FlashBackDatabaseCommand represents FLASHBACK DATABASE statement
	FlashBackDatabaseCommand = "FLASHBACK DATABASE"
	// FlashBackTableCommand represents FLASHBACK TABLE statement
	FlashBackTableCommand = "FLASHBACK TABLE"
	// FlashBackClusterCommand represents FLASHBACK CLUSTER statement
	FlashBackClusterCommand = "FLASHBACK CLUSTER"
	// LockTablesCommand represents LOCK TABLES statement
	LockTablesCommand = "LOCK TABLES"
	// OptimizeTableCommand represents OPTIMIZE TABLE statement
	OptimizeTableCommand = "OPTIMIZE TABLE"
	// RecoverTableCommand represents RECOVER TABLE statement
	RecoverTableCommand = "RECOVER TABLE"
	// RenameTableCommand represents RENAME TABLE statement
	RenameTableCommand = "RENAME TABLE"
	// RenameUserCommand represents RENAME USER statement
	RenameUserCommand = "RENAME USER"
	// AdminRepairTableCommand represents ADMIN REPAIR TABLE statement
	AdminRepairTableCommand = "ADMIN REPAIR TABLE"
	// TruncateTableCommand represents TRUNCATE TABLE statement
	TruncateTableCommand = "TRUNCATE TABLE"
	// UnlockTablesCommand represents UNLOCK TABLES statement
	UnlockTablesCommand = "UNLOCK TABLES"
)

// DML Statements

const (
	// CallCommand represents CALL statement
	CallCommand = "CALL"
	// DeleteCommand represents DELETE statement
	DeleteCommand = "DELETE"
	// DistributeTableCommand represents DISTRIBUTE TABLE statement
	DistributeTableCommand = "DISTRIBUTE TABLE"
	// ImportIntoCommand represents IMPORT INTO statement
	ImportIntoCommand = "IMPORT INTO"
	// ReplaceCommand represents REPLACE statement
	ReplaceCommand = "REPLACE"
	// InsertCommand represents INSERT statement
	InsertCommand = "INSERT"
	// LoadDataCommand represents LOAD DATA statement
	LoadDataCommand = "LOAD DATA"
	// BatchCommand represents BATCH statement
	BatchCommand = "BATCH"
	// SelectCommand represents SELECT statement
	SelectCommand = "SELECT"
	// SplitRegionCommand represents SPLIT REGION statement
	SplitRegionCommand = "SPLIT REGION"
	// UpdateCommand represents UPDATE statement
	UpdateCommand = "UPDATE"
)

// Show Statements

const (
	// ShowCommand represents SHOW statement
	ShowCommand = "SHOW"
	// ShowCreateTableCommand represents SHOW CREATE TABLE statement
	ShowCreateTableCommand = "SHOW CREATE TABLE"
	// ShowCreateViewCommand represents SHOW CREATE VIEW statement
	ShowCreateViewCommand = "SHOW CREATE VIEW"
	// ShowCreateDatabaseCommand represents SHOW CREATE DATABASE statement
	ShowCreateDatabaseCommand = "SHOW CREATE DATABASE"
	// ShowCreateUserCommand represents SHOW CREATE USER statement
	ShowCreateUserCommand = "SHOW CREATE USER"
	// ShowCreateSequenceCommand represents SHOW CREATE SEQUENCE statement
	ShowCreateSequenceCommand = "SHOW CREATE SEQUENCE"
	// ShowCreatePlacementPolicyCommand represents SHOW CREATE PLACEMENT POLICY statement
	ShowCreatePlacementPolicyCommand = "SHOW CREATE PLACEMENT POLICY"
	// ShowCreateResourceGroupCommand represents SHOW CREATE RESOURCE GROUP statement
	ShowCreateResourceGroupCommand = "SHOW CREATE RESOURCE GROUP"
	// ShowCreateProcedureCommand represents SHOW CREATE PROCEDURE statement
	ShowCreateProcedureCommand = "SHOW CREATE PROCEDURE"
	// ShowDatabasesCommand represents SHOW DATABASES statement
	ShowDatabasesCommand = "SHOW DATABASES"
	// ShowTableCommand represents SHOW TABLES statement
	ShowTableCommand = "SHOW TABLE"
	// ShowTableStatusCommand represents SHOW TABLE STATUS statement
	ShowTableStatusCommand = "SHOW TABLE STATUS"
	// ShowColumnsCommand represents SHOW COLUMNS statement
	ShowColumnsCommand = "SHOW COLUMNS"
	// ShowIndexCommand represents SHOW INDEX statement
	ShowIndexCommand = "SHOW INDEX"
	// ShowVariablesCommand represents SHOW VARIABLES statement
	ShowVariablesCommand = "SHOW VARIABLES"
	// ShowStatusCommand represents SHOW STATUS statement
	ShowStatusCommand = "SHOW STATUS"
	// ShowProcessListCommand represents SHOW PROCESSLIST statement
	ShowProcessListCommand = "SHOW PROCESSLIST"
	// ShowEnginesCommand represents SHOW ENGINES statement
	ShowEnginesCommand = "SHOW ENGINES"
	// ShowCharsetCommand represents SHOW CHARSET statement
	ShowCharsetCommand = "SHOW CHARSET"
	// ShowCollationCommand represents SHOW COLLATION statement
	ShowCollationCommand = "SHOW COLLATION"
	// ShowWarningsCommand represents SHOW WARNINGS statement
	ShowWarningsCommand = "SHOW WARNINGS"
	// ShowErrorsCommand represents SHOW ERRORS statement
	ShowErrorsCommand = "SHOW ERRORS"
	// ShowGrantsCommand represents SHOW GRANTS statement
	ShowGrantsCommand = "SHOW GRANTS"
	// ShowPrivilegesCommand represents SHOW PRIVILEGES statement
	ShowPrivilegesCommand = "SHOW PRIVILEGES"
	// ShowTriggersCommand represents SHOW TRIGGERS statement
	ShowTriggersCommand = "SHOW TRIGGERS"
	// ShowProcedureStatusCommand represents SHOW PROCEDURE STATUS statement
	ShowProcedureStatusCommand = "SHOW PROCEDURE STATUS"
	// ShowFunctionStatusCommand represents SHOW FUNCTION STATUS statement
	ShowFunctionStatusCommand = "SHOW FUNCTION STATUS"
	// ShowEventsCommand represents SHOW EVENTS statement
	ShowEventsCommand = "SHOW EVENTS"
	// ShowPluginsCommand represents SHOW PLUGINS statement
	ShowPluginsCommand = "SHOW PLUGINS"
	// ShowProfileCommand represents SHOW PROFILE statement
	ShowProfileCommand = "SHOW PROFILE"
	// ShowProfilesCommand represents SHOW PROFILES statement
	ShowProfilesCommand = "SHOW PROFILES"
	// ShowMasterStatusCommand represents SHOW MASTER STATUS statement
	ShowMasterStatusCommand = "SHOW MASTER STATUS"
	// ShowBinaryLogStatusCommand represents SHOW BINARY LOG STATUS statement
	ShowBinaryLogStatusCommand = "SHOW BINARY LOG STATUS"
	// ShowOpenTablesCommand represents SHOW OPEN TABLES statement
	ShowOpenTablesCommand = "SHOW OPEN TABLES"
	// ShowConfigCommand represents SHOW CONFIG statement
	ShowConfigCommand = "SHOW CONFIG"
	// ShowStatsExtendedCommand represents SHOW STATS_EXTENDED statement
	ShowStatsExtendedCommand = "SHOW STATS_EXTENDED"
	// ShowStatsMetaCommand represents SHOW STATS_META statement
	ShowStatsMetaCommand = "SHOW STATS_META"
	// ShowStatsHistogramsCommand represents SHOW STATS_HISTOGRAMS statement
	ShowStatsHistogramsCommand = "SHOW STATS_HISTOGRAMS"
	// ShowStatsTopNCommand represents SHOW STATS_TOPN statement
	ShowStatsTopNCommand = "SHOW STATS_TOPN"
	// ShowStatsBucketsCommand represents SHOW STATS_BUCKETS statement
	ShowStatsBucketsCommand = "SHOW STATS_BUCKETS"
	// ShowStatsHealthyCommand represents SHOW STATS_HEALTHY statement
	ShowStatsHealthyCommand = "SHOW STATS_HEALTHY"
	// ShowStatsLockedCommand represents SHOW STATS_LOCKED statement
	ShowStatsLockedCommand = "SHOW STATS_LOCKED"
	// ShowHistogramsInFlightCommand represents SHOW HISTOGRAMS_IN_FLIGHT statement
	ShowHistogramsInFlightCommand = "SHOW HISTOGRAMS_IN_FLIGHT"
	// ShowColumnStatsUsageCommand represents SHOW COLUMN_STATS_USAGE statement
	ShowColumnStatsUsageCommand = "SHOW COLUMN_STATS_USAGE"
	// ShowBindingsCommand represents SHOW BINDINGS statement
	ShowBindingsCommand = "SHOW BINDINGS"
	// ShowBindingCacheStatusCommand represents SHOW BINDING_CACHE STATUS statement
	ShowBindingCacheStatusCommand = "SHOW BINDING_CACHE STATUS"
	// ShowAnalyzeStatusCommand represents SHOW ANALYZE STATUS statement
	ShowAnalyzeStatusCommand = "SHOW ANALYZE STATUS"
	// ShowRegionsCommand represents SHOW TABLE REGIONS statement
	ShowRegionsCommand = "SHOW TABLE REGIONS"
	// ShowBuiltinsCommand represents SHOW BUILTINS statement
	ShowBuiltinsCommand = "SHOW BUILTINS"
	// ShowTableNextRowIdCommand represents SHOW TABLE NEXT_ROW_ID statement
	ShowTableNextRowIdCommand = "SHOW TABLE NEXT_ROW_ID"
	// ShowBackupsCommand represents SHOW BACKUPS statement
	ShowBackupsCommand = "SHOW BACKUPS"
	// ShowRestoresCommand represents SHOW RESTORES statement
	ShowRestoresCommand = "SHOW RESTORES"
	// ShowImportsCommand represents SHOW IMPORTS statement
	ShowImportsCommand = "SHOW IMPORTS"
	// ShowCreateImportCommand represents SHOW CREATE IMPORT statement
	ShowCreateImportCommand = "SHOW CREATE IMPORT"
	// ShowImportJobsCommand represents SHOW IMPORT JOBS statement
	ShowImportJobsCommand = "SHOW IMPORT JOBS"
	// ShowImportGroupsCommand represents SHOW IMPORT GROUPS statement
	ShowImportGroupsCommand = "SHOW IMPORT GROUPS"
	// ShowPlacementCommand represents SHOW PLACEMENT statement
	ShowPlacementCommand = "SHOW PLACEMENT"
	// ShowPlacementForDatabaseCommand represents SHOW PLACEMENT FOR DATABASE statement
	ShowPlacementForDatabaseCommand = "SHOW PLACEMENT FOR DATABASE"
	// ShowPlacementForTableCommand represents SHOW PLACEMENT FOR TABLE statement
	ShowPlacementForTableCommand = "SHOW PLACEMENT FOR TABLE"
	// ShowPlacementForPartitionCommand represents SHOW PLACEMENT FOR PARTITION statement
	ShowPlacementForPartitionCommand = "SHOW PLACEMENT FOR PARTITION"
	// ShowPlacementLabelsCommand represents SHOW PLACEMENT LABELS statement
	ShowPlacementLabelsCommand = "SHOW PLACEMENT LABELS"
	// ShowSessionStatesCommand represents SHOW SESSION_STATES statement
	ShowSessionStatesCommand = "SHOW SESSION_STATES"
	// ShowDistributionsCommand represents SHOW DISTRIBUTIONS statement
	ShowDistributionsCommand = "SHOW DISTRIBUTIONS"
	// ShowPlanCommand represents SHOW PLAN statement
	ShowPlanCommand = "SHOW PLAN"
	// ShowDistributionJobsCommand represents SHOW DISTRIBUTION JOBS statement
	ShowDistributionJobsCommand = "SHOW DISTRIBUTION JOB"
	// ShowAffinityCommand represents SHOW AFFINITY statement
	ShowAffinityCommand = "SHOW AFFINITY"
)

// Admin Commands

const (
	// AdminShowDDLCommand represents ADMIN SHOW DDL statement
	AdminShowDDLCommand = "ADMIN SHOW DDL"
	// AdminCheckTableCommand represents ADMIN CHECK TABLE statement
	AdminCheckTableCommand = "ADMIN CHECK TABLE"
	// AdminShowDDLJobsCommand represents ADMIN SHOW DDL JOBS statement
	AdminShowDDLJobsCommand = "ADMIN SHOW DDL JOBS"
	// AdminCancelDDLJobsCommand represents ADMIN CANCEL DDL JOBS statement
	AdminCancelDDLJobsCommand = "ADMIN CANCEL DDL JOBS"
	// AdminPauseDDLJobsCommand represents ADMIN PAUSE DDL JOBS statement
	AdminPauseDDLJobsCommand = "ADMIN PAUSE DDL JOBS"
	// AdminResumeDDLJobsCommand represents ADMIN RESUME DDL JOBS statement
	AdminResumeDDLJobsCommand = "ADMIN RESUME DDL JOBS"
	// AdminCheckIndexCommand represents ADMIN CHECK INDEX statement
	AdminCheckIndexCommand = "ADMIN CHECK INDEX"
	// AdminRecoverIndexCommand represents ADMIN RECOVER INDEX statement
	AdminRecoverIndexCommand = "ADMIN RECOVER INDEX"
	// AdminCleanupIndexCommand represents ADMIN CLEANUP INDEX statement
	AdminCleanupIndexCommand = "ADMIN CLEANUP INDEX"
	// AdminCheckIndexRangeCommand represents ADMIN CHECK INDEX RANGE statement
	AdminCheckIndexRangeCommand = "ADMIN CHECK INDEX RANGE"
	// AdminShowDDLJobQueriesCommand represents ADMIN SHOW DDL JOB QUERIES statement
	AdminShowDDLJobQueriesCommand = "ADMIN SHOW DDL JOB QUERIES"
	// AdminChecksumTableCommand represents ADMIN CHECKSUM TABLE statement
	AdminChecksumTableCommand = "ADMIN CHECKSUM TABLE"
	// AdminShowSlowCommand represents ADMIN SHOW SLOW statement
	AdminShowSlowCommand = "ADMIN SHOW SLOW"
	// AdminShowNextRowIDCommand represents ADMIN SHOW NEXT_ROW_ID statement
	AdminShowNextRowIDCommand = "ADMIN SHOW NEXT_ROW_ID"
	// AdminReloadExprPushdownBlacklistCommand represents ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST statement
	AdminReloadExprPushdownBlacklistCommand = "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST"
	// AdminReloadOptRuleBlacklistCommand represents ADMIN RELOAD OPT_RULE_BLACKLIST statement
	AdminReloadOptRuleBlacklistCommand = "ADMIN RELOAD OPT_RULE_BLACKLIST"
	// AdminPluginsDisableCommand represents ADMIN PLUGINS DISABLE statement
	AdminPluginsDisableCommand = "ADMIN PLUGINS DISABLE"
	// AdminPluginsEnableCommand represents ADMIN PLUGINS ENABLE statement
	AdminPluginsEnableCommand = "ADMIN PLUGINS ENABLE"
	// AdminFlushBindingsCommand represents ADMIN FLUSH BINDINGS statement
	AdminFlushBindingsCommand = "ADMIN FLUSH BINDINGS"
	// AdminCaptureBindingsCommand represents ADMIN CAPTURE BINDINGS statement
	AdminCaptureBindingsCommand = "ADMIN CAPTURE BINDINGS"
	// AdminEvolveBindingsCommand represents ADMIN EVOLVE BINDINGS statement
	AdminEvolveBindingsCommand = "ADMIN EVOLVE BINDINGS"
	// AdminReloadBindingsCommand represents ADMIN RELOAD BINDINGS statement
	AdminReloadBindingsCommand = "ADMIN RELOAD BINDINGS"
	// AdminReloadStatsExtendedCommand represents ADMIN RELOAD STATS_EXTENDED statement
	AdminReloadStatsExtendedCommand = "ADMIN RELOAD STATS_EXTENDED"
	// AdminFlushPlanCacheCommand represents ADMIN FLUSH PLAN_CACHE statement
	AdminFlushPlanCacheCommand = "ADMIN FLUSH PLAN_CACHE"
	// AdminSetBDRRoleCommand represents ADMIN SET BDR ROLE statement
	AdminSetBDRRoleCommand = "ADMIN SET BDR ROLE"
	// AdminShowBDRRoleCommand represents ADMIN SHOW BDR ROLE statement
	AdminShowBDRRoleCommand = "ADMIN SHOW BDR ROLE"
	// AdminUnsetBDRRoleCommand represents ADMIN UNSET BDR ROLE statement
	AdminUnsetBDRRoleCommand = "ADMIN UNSET BDR ROLE"
	// AdminAlterDDLJobsCommand represents ADMIN ALTER DDL JOBS statement
	AdminAlterDDLJobsCommand = "ADMIN ALTER DDL JOBS"
	// AdminCreateWorkloadSnapshotCommand represents ADMIN CREATE WORKLOAD SNAPSHOT statement
	AdminCreateWorkloadSnapshotCommand = "ADMIN CREATE WORKLOAD SNAPSHOT"
)

// BRIE Commands

const (
	// BackupCommand represents BACKUP statement
	BackupCommand = "BACKUP"
	// RestoreCommand represents RESTORE statement
	RestoreCommand = "RESTORE"
	// RestorePITCommand represents RESTORE POINT IN TIME statement
	RestorePITCommand = "RESTORE POINT"
	// StreamStartCommand represents STREAM START statement
	StreamStartCommand = "BACKUP LOGS"
	// StreamStopCommand represents STREAM STOP statement
	StreamStopCommand = "STOP BACKUP LOGS"
	// StreamPauseCommand represents STREAM PAUSE statement
	StreamPauseCommand = "PAUSE BACKUP LOGS"
	// StreamResumeCommand represents STREAM RESUME statement
	StreamResumeCommand = "RESUME BACKUP LOGS"
	// StreamStatusCommand represents STREAM STATUS statement
	StreamStatusCommand = "SHOW BACKUP LOGS STATUS"
	// StreamMetaDataCommand represents STREAM METADATA statement
	StreamMetaDataCommand = "SHOW BACKUP LOGS METADATA"
	// StreamPurgeCommand represents STREAM PURGE statement
	StreamPurgeCommand = "PURGE BACKUP LOGS"
	// ShowBRJobCommand represents SHOW BR JOB statement
	ShowBRJobCommand = "SHOW BR JOB"
	// ShowBRJobQueryCommand represents SHOW BR JOB QUERY statement
	ShowBRJobQueryCommand = "SHOW BR JOB QUERY"
	// CancelBRJobCommand represents CANCEL BR JOB statement
	CancelBRJobCommand = "CANCEL BR JOB"
	// ShowBackupMetaCommand represents SHOW BACKUP META statement
	ShowBackupMetaCommand = "SHOW BACKUP META"
)

// Miscellaneous Commands

const (
	// AddQueryWatchCommand represents ADD QUERY WATCH statement
	AddQueryWatchCommand = "ADD QUERY WATCH"
	// AnalyzeTableCommand represents ANALYZE TABLE statement
	AnalyzeTableCommand = "ANALYZE TABLE"
	// BeginCommand represents BEGIN statement
	BeginCommand = "BEGIN"
	// BinlogCommand represents BINLOG statement
	BinlogCommand = "BINLOG"
	// CalibrateResourceCommand represents CALIBRATE RESOURCE statement
	CalibrateResourceCommand = "CALIBRATE RESOURCE"
	// CancelDistributionJobCommand represents CANCEL DISTRIBUTION JOB statement
	CancelDistributionJobCommand = "CANCEL DISTRIBUTION JOB"
	// CommitCommand represents COMMIT statement
	CommitCommand = "COMMIT"
	// AlterTableCompactCommand represents ALTER TABLE COMPACT statement
	AlterTableCompactCommand = "ALTER TABLE COMPACT"
	// CreateBindingCommand represents CREATE BINDING statement
	CreateBindingCommand = "CREATE BINDING"
	// CreateStatisticsCommand represents CREATE STATISTICS statement
	CreateStatisticsCommand = "CREATE STATISTICS"
	// DeallocateCommand represents DEALLOCATE statement
	DeallocateCommand = "DEALLOCATE"
	// DoCommand represents DO statement
	DoCommand = "DO"
	// DropBindingCommand represents DROP BINDING statement
	DropBindingCommand = "DROP BINDING"
	// DropQueryWatchCommand represents DROP QUERY WATCH statement
	DropQueryWatchCommand = "DROP QUERY WATCH"
	// DropStatisticsCommand represents DROP STATISTICS statement
	DropStatisticsCommand = "DROP STATISTICS"
	// ExecuteCommand represents EXECUTE statement
	ExecuteCommand = "EXECUTE"
	// ExplainForConnectionCommand represents EXPLAIN FOR CONNECTION statement
	ExplainForConnectionCommand = "EXPLAIN FOR CONNECTION"
	// ExplainAnalyzeCommand represents EXPLAIN ANALYZE statement
	ExplainAnalyzeCommand = "EXPLAIN ANALYZE"
	// ExplainCommand represents EXPLAIN statement
	ExplainCommand = "EXPLAIN"
	// FlushCommand represents FLUSH statement
	FlushCommand = "FLUSH"
	// GrantCommand represents GRANT statement
	GrantCommand = "GRANT"
	// GrantProxyCommand represents GRANT PROXY statement
	GrantProxyCommand = "GRANT PROXY"
	// GrantRoleCommand represents GRANT ROLE statement
	GrantRoleCommand = "GRANT ROLE"
	// HelpCommand represents HELP statement
	HelpCommand = "HELP"
	// CancelImportIntoJobCommand represents CANCEL IMPORT INTO JOB statement
	CancelImportIntoJobCommand = "CANCEL IMPORT INTO JOB"
	// KillCommand represents KILL statement
	KillCommand = "KILL"
	// PlanReplayerCommand represents PLAN REPLAYER statement
	PlanReplayerCommand = "PLAN REPLAYER"
	// PrepareCommand represents PREPARE statement
	PrepareCommand = "PREPARE"
	// ReleaseSavepointCommand represents RELEASE SAVEPOINT statement
	ReleaseSavepointCommand = "RELEASE SAVEPOINT"
	// RestartCommand represents RESTART statement
	RestartCommand = "RESTART"
	// RevokeCommand represents REVOKE statement
	RevokeCommand = "REVOKE"
	// RevokeRoleCommand represents REVOKE ROLE statement
	RevokeRoleCommand = "REVOKE ROLE"
	// RollbackCommand represents ROLLBACK statement
	RollbackCommand = "ROLLBACK"
	// SavepointCommand represents SAVEPOINT statement
	SavepointCommand = "SAVEPOINT"
	// SetBindingCommand represents SET BINDING statement
	SetBindingCommand = "SET BINDING"
	// SetConfigCommand represents SET CONFIG statement
	SetConfigCommand = "SET CONFIG"
	// SetDefaultRoleCommand represents SET DEFAULT ROLE statement
	SetDefaultRoleCommand = "SET DEFAULT ROLE"
	// SetPasswordCommand represents SET PASSWORD statement
	SetPasswordCommand = "SET PASSWORD"
	// SetResourceGroupCommand represents SET RESOURCE GROUP statement
	SetResourceGroupCommand = "SET RESOURCE GROUP"
	// SetRoleCommand represents SET ROLE statement
	SetRoleCommand = "SET ROLE"
	// SetSessionStatesCommand represents SET SESSION_STATES statement
	SetSessionStatesCommand = "SET SESSION_STATES"
	// SetCommand represents SET statement
	SetCommand = "SET"
	// ShutdownCommand represents SHUTDOWN statement
	ShutdownCommand = "SHUTDOWN"
	// TraceCommand represents TRACE statement
	TraceCommand = "TRACE"
	// TrafficCommand represents TRAFFIC statement
	TrafficCommand = "TRAFFIC"
	// UseCommand represents USE statement
	UseCommand = "USE"
	// LoadStatsCommand represents LOAD STATS statement
	LoadStatsCommand = "LOAD STATS"
	// DropStatsCommand represents DROP STATS statement
	DropStatsCommand = "DROP STATS"
	// LockStatsCommand represents LOCK STATS statement
	LockStatsCommand = "LOCK STATS"
	// UnlockStatsCommand represents UNLOCK STATS statement
	UnlockStatsCommand = "UNLOCK STATS"
	// RefreshStatsCommand represents REFRESH STATS statement
	RefreshStatsCommand = "REFRESH STATS"
	// RecommendIndexCommand represents RECOMMEND INDEX statement
	RecommendIndexCommand = "RECOMMEND INDEX"
	// ProcedureCommand represents all statements in procedure. It's too rough
	// but still fine for now.
	ProcedureCommand = "PROCEDURE"
	// UnknownCommand represents unknown statements
	UnknownCommand = "UNKNOWN"
	// SetOprCommand represents UNION/INTERSECT/EXCEPT statement
	SetOprCommand = "SET OPERATION"
)

// SEMCommand returns the command string for the statement.
func (n *AlterDatabaseStmt) SEMCommand() string {
	return AlterDatabaseCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterInstanceStmt) SEMCommand() string {
	return AlterInstanceCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterPlacementPolicyStmt) SEMCommand() string {
	return AlterPlacementPolicyCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterRangeStmt) SEMCommand() string {
	return AlterRangeCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterResourceGroupStmt) SEMCommand() string {
	return AlterResourceGroupCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterSequenceStmt) SEMCommand() string {
	return AlterSequenceCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterTableStmt) SEMCommand() string {
	return AlterTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *AlterUserStmt) SEMCommand() string {
	return AlterUserCommand
}

// SEMCommand returns the command string for the statement.
func (n *CleanupTableLockStmt) SEMCommand() string {
	return AdminCleanupTableLockCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateDatabaseStmt) SEMCommand() string {
	return CreateDatabaseCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateIndexStmt) SEMCommand() string {
	return CreateIndexCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreatePlacementPolicyStmt) SEMCommand() string {
	return CreatePlacementPolicyCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateResourceGroupStmt) SEMCommand() string {
	return CreateResourceGroupCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateSequenceStmt) SEMCommand() string {
	return CreateSequenceCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateTableStmt) SEMCommand() string {
	return CreateTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateUserStmt) SEMCommand() string {
	return CreateUserCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateViewStmt) SEMCommand() string {
	return CreateViewCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateMaterializedViewLogStmt) SEMCommand() string {
	return CreateMaterializedViewLogCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateMaterializedViewStmt) SEMCommand() string {
	return CreateMaterializedViewCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropDatabaseStmt) SEMCommand() string {
	return DropDatabaseCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropIndexStmt) SEMCommand() string {
	return DropIndexCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropPlacementPolicyStmt) SEMCommand() string {
	return DropPlacementPolicyCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropResourceGroupStmt) SEMCommand() string {
	return DropResourceGroupCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropSequenceStmt) SEMCommand() string {
	return DropSequenceCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropTableStmt) SEMCommand() string {
	if n.IsView {
		return DropViewCommand
	}
	return DropTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropMaterializedViewLogStmt) SEMCommand() string {
	return DropMaterializedViewLogCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropMaterializedViewStmt) SEMCommand() string {
	return DropMaterializedViewCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropUserStmt) SEMCommand() string {
	return DropUserCommand
}

// SEMCommand returns the command string for the statement.
func (n *FlashBackDatabaseStmt) SEMCommand() string {
	return FlashBackDatabaseCommand
}

// SEMCommand returns the command string for the statement.
func (n *FlashBackTableStmt) SEMCommand() string {
	return FlashBackTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *FlashBackToTimestampStmt) SEMCommand() string {
	return FlashBackClusterCommand
}

// SEMCommand returns the command string for the statement.
func (n *LockTablesStmt) SEMCommand() string {
	return LockTablesCommand
}

// SEMCommand returns the command string for the statement.
func (n *OptimizeTableStmt) SEMCommand() string {
	return OptimizeTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *RecoverTableStmt) SEMCommand() string {
	return RecoverTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *RenameTableStmt) SEMCommand() string {
	return RenameTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *RenameUserStmt) SEMCommand() string {
	return RenameUserCommand
}

// SEMCommand returns the command string for the statement.
func (n *RepairTableStmt) SEMCommand() string {
	return AdminRepairTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *TruncateTableStmt) SEMCommand() string {
	return TruncateTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *UnlockTablesStmt) SEMCommand() string {
	return UnlockTablesCommand
}

// DML Statements

// SEMCommand returns the command string for the statement.
func (n *CallStmt) SEMCommand() string {
	return CallCommand
}

// SEMCommand returns the command string for the statement.
func (n *DeleteStmt) SEMCommand() string {
	return DeleteCommand
}

// SEMCommand returns the command string for the statement.
func (n *DistributeTableStmt) SEMCommand() string {
	return DistributeTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *ImportIntoStmt) SEMCommand() string {
	return ImportIntoCommand
}

// SEMCommand returns the command string for the statement.
func (n *InsertStmt) SEMCommand() string {
	if n.IsReplace {
		return ReplaceCommand
	}
	return InsertCommand
}

// SEMCommand returns the command string for the statement.
func (n *LoadDataStmt) SEMCommand() string {
	return LoadDataCommand
}

// SEMCommand returns the command string for the statement.
func (n *NonTransactionalDMLStmt) SEMCommand() string {
	return BatchCommand
}

// SEMCommand returns the command string for the statement.
func (n *SelectStmt) SEMCommand() string {
	return SelectCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetOprStmt) SEMCommand() string {
	return SetOprCommand
}

// SEMCommand returns the command string for the statement.
func (n *ShowStmt) SEMCommand() string {
	switch n.Tp {
	case ShowCreateTable:
		return ShowCreateTableCommand
	case ShowCreateView:
		return ShowCreateViewCommand
	case ShowCreateDatabase:
		return ShowCreateDatabaseCommand
	case ShowCreateUser:
		return ShowCreateUserCommand
	case ShowCreateSequence:
		return ShowCreateSequenceCommand
	case ShowCreatePlacementPolicy:
		return ShowCreatePlacementPolicyCommand
	case ShowCreateResourceGroup:
		return ShowCreateResourceGroupCommand
	case ShowCreateProcedure:
		return ShowCreateProcedureCommand
	case ShowDatabases:
		return ShowDatabasesCommand
	case ShowTables:
		return ShowTableCommand
	case ShowTableStatus:
		return ShowTableStatusCommand
	case ShowColumns:
		return ShowColumnsCommand
	case ShowIndex:
		return ShowIndexCommand
	case ShowVariables:
		return ShowVariablesCommand
	case ShowStatus:
		return ShowStatusCommand
	case ShowProcessList:
		return ShowProcessListCommand
	case ShowEngines:
		return ShowEnginesCommand
	case ShowCharset:
		return ShowCharsetCommand
	case ShowCollation:
		return ShowCollationCommand
	case ShowWarnings:
		return ShowWarningsCommand
	case ShowErrors:
		return ShowErrorsCommand
	case ShowGrants:
		return ShowGrantsCommand
	case ShowPrivileges:
		return ShowPrivilegesCommand
	case ShowTriggers:
		return ShowTriggersCommand
	case ShowProcedureStatus:
		return ShowProcedureStatusCommand
	case ShowFunctionStatus:
		return ShowFunctionStatusCommand
	case ShowEvents:
		return ShowEventsCommand
	case ShowPlugins:
		return ShowPluginsCommand
	case ShowProfile:
		return ShowProfileCommand
	case ShowProfiles:
		return ShowProfilesCommand
	case ShowMasterStatus:
		return ShowMasterStatusCommand
	case ShowBinlogStatus:
		return ShowBinaryLogStatusCommand
	case ShowReplicaStatus:
		return ShowCommand
	case ShowOpenTables:
		return ShowOpenTablesCommand
	case ShowConfig:
		return ShowConfigCommand
	case ShowStatsExtended:
		return ShowStatsExtendedCommand
	case ShowStatsMeta:
		return ShowStatsMetaCommand
	case ShowStatsHistograms:
		return ShowStatsHistogramsCommand
	case ShowStatsTopN:
		return ShowStatsTopNCommand
	case ShowStatsBuckets:
		return ShowStatsBucketsCommand
	case ShowStatsHealthy:
		return ShowStatsHealthyCommand
	case ShowStatsLocked:
		return ShowStatsLockedCommand
	case ShowHistogramsInFlight:
		return ShowHistogramsInFlightCommand
	case ShowColumnStatsUsage:
		return ShowColumnStatsUsageCommand
	case ShowBindings:
		return ShowBindingsCommand
	case ShowBindingCacheStatus:
		return ShowBindingCacheStatusCommand
	case ShowAnalyzeStatus:
		return ShowAnalyzeStatusCommand
	case ShowRegions:
		return ShowRegionsCommand
	case ShowBuiltins:
		return ShowBuiltinsCommand
	case ShowTableNextRowId:
		return ShowTableNextRowIdCommand
	case ShowBackups:
		return ShowBackupsCommand
	case ShowRestores:
		return ShowRestoresCommand
	case ShowImports:
		return ShowImportsCommand
	case ShowCreateImport:
		return ShowCreateImportCommand
	case ShowImportJobs:
		return ShowImportJobsCommand
	case ShowImportGroups:
		return ShowImportGroupsCommand
	case ShowPlacement:
		return ShowPlacementCommand
	case ShowPlacementForDatabase:
		return ShowPlacementForDatabaseCommand
	case ShowPlacementForTable:
		return ShowPlacementForTableCommand
	case ShowPlacementForPartition:
		return ShowPlacementForPartitionCommand
	case ShowPlacementLabels:
		return ShowPlacementLabelsCommand
	case ShowSessionStates:
		return ShowSessionStatesCommand
	case ShowDistributions:
		return ShowDistributionsCommand
	case ShowDistributionJobs:
		return ShowDistributionJobsCommand
	case ShowAffinity:
		return ShowAffinityCommand
	default:
		return UnknownCommand
	}
}

// SEMCommand returns the command string for the statement.
func (n *SplitRegionStmt) SEMCommand() string {
	return SplitRegionCommand
}

// SEMCommand returns the command string for the statement.
func (n *UpdateStmt) SEMCommand() string {
	return UpdateCommand
}

// Miscellaneous Statements

// SEMCommand returns the command string for the statement.
func (n *AddQueryWatchStmt) SEMCommand() string {
	return AddQueryWatchCommand
}

// SEMCommand returns the command string for the statement.
func (n *AdminStmt) SEMCommand() string {
	switch n.Tp {
	case AdminShowDDL:
		return AdminShowDDLCommand
	case AdminCheckTable:
		return AdminCheckTableCommand
	case AdminShowDDLJobs:
		return AdminShowDDLJobsCommand
	case AdminCancelDDLJobs:
		return AdminCancelDDLJobsCommand
	case AdminPauseDDLJobs:
		return AdminPauseDDLJobsCommand
	case AdminResumeDDLJobs:
		return AdminResumeDDLJobsCommand
	case AdminCheckIndex:
		return AdminCheckIndexCommand
	case AdminRecoverIndex:
		return AdminRecoverIndexCommand
	case AdminCleanupIndex:
		return AdminCleanupIndexCommand
	case AdminCheckIndexRange:
		return AdminCheckIndexRangeCommand
	case AdminShowDDLJobQueries:
		return AdminShowDDLJobQueriesCommand
	case AdminShowDDLJobQueriesWithRange:
		return AdminShowDDLJobQueriesCommand
	case AdminChecksumTable:
		return AdminChecksumTableCommand
	case AdminShowSlow:
		return AdminShowSlowCommand
	case AdminShowNextRowID:
		return AdminShowNextRowIDCommand
	case AdminReloadExprPushdownBlacklist:
		return AdminReloadExprPushdownBlacklistCommand
	case AdminReloadOptRuleBlacklist:
		return AdminReloadOptRuleBlacklistCommand
	case AdminPluginDisable:
		return AdminPluginsDisableCommand
	case AdminPluginEnable:
		return AdminPluginsEnableCommand
	case AdminFlushBindings:
		return AdminFlushBindingsCommand
	case AdminCaptureBindings:
		return AdminCaptureBindingsCommand
	case AdminEvolveBindings:
		return AdminEvolveBindingsCommand
	case AdminReloadBindings:
		return AdminReloadBindingsCommand
	case AdminReloadStatistics:
		return AdminReloadStatsExtendedCommand
	case AdminFlushPlanCache:
		return AdminFlushPlanCacheCommand
	case AdminSetBDRRole:
		return AdminSetBDRRoleCommand
	case AdminShowBDRRole:
		return AdminShowBDRRoleCommand
	case AdminUnsetBDRRole:
		return AdminUnsetBDRRoleCommand
	case AdminAlterDDLJob:
		return AdminAlterDDLJobsCommand
	case AdminWorkloadRepoCreate:
		return AdminCreateWorkloadSnapshotCommand
	default:
		return UnknownCommand
	}
}

// SEMCommand returns the command string for the statement.
func (n *AnalyzeTableStmt) SEMCommand() string {
	return AnalyzeTableCommand
}

// SEMCommand returns the command string for the statement.
func (n *BeginStmt) SEMCommand() string {
	return BeginCommand
}

// SEMCommand returns the command string for the statement.
func (n *BinlogStmt) SEMCommand() string {
	return BinlogCommand
}

// SEMCommand returns the command string for the statement.
func (n *BRIEStmt) SEMCommand() string {
	switch n.Kind {
	case BRIEKindBackup:
		return BackupCommand
	case BRIEKindRestore:
		return RestoreCommand
	case BRIEKindRestorePIT:
		return RestorePITCommand
	case BRIEKindStreamStart:
		return StreamStartCommand
	case BRIEKindStreamStop:
		return StreamStopCommand
	case BRIEKindStreamPause:
		return StreamPauseCommand
	case BRIEKindStreamResume:
		return StreamResumeCommand
	case BRIEKindStreamStatus:
		return StreamStatusCommand
	case BRIEKindStreamMetaData:
		return StreamMetaDataCommand
	case BRIEKindStreamPurge:
		return StreamPurgeCommand
	case BRIEKindShowJob:
		return ShowBRJobCommand
	case BRIEKindShowQuery:
		return ShowBRJobQueryCommand
	case BRIEKindCancelJob:
		return CancelBRJobCommand
	case BRIEKindShowBackupMeta:
		return ShowBackupMetaCommand
	default:
		return UnknownCommand
	}
}

// SEMCommand returns the command string for the statement.
func (n *CalibrateResourceStmt) SEMCommand() string {
	return CalibrateResourceCommand
}

// SEMCommand returns the command string for the statement.
func (n *CancelDistributionJobStmt) SEMCommand() string {
	return CancelDistributionJobCommand
}

// SEMCommand returns the command string for the statement.
func (n *CommitStmt) SEMCommand() string {
	return CommitCommand
}

// SEMCommand returns the command string for the statement.
func (n *CompactTableStmt) SEMCommand() string {
	return AlterTableCompactCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateBindingStmt) SEMCommand() string {
	return CreateBindingCommand
}

// SEMCommand returns the command string for the statement.
func (n *CreateStatisticsStmt) SEMCommand() string {
	return CreateStatisticsCommand
}

// SEMCommand returns the command string for the statement.
func (n *DeallocateStmt) SEMCommand() string {
	return DeallocateCommand
}

// SEMCommand returns the command string for the statement.
func (n *DoStmt) SEMCommand() string {
	return DoCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropBindingStmt) SEMCommand() string {
	return DropBindingCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropQueryWatchStmt) SEMCommand() string {
	return DropQueryWatchCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropStatisticsStmt) SEMCommand() string {
	return DropStatisticsCommand
}

// SEMCommand returns the command string for the statement.
func (n *ExecuteStmt) SEMCommand() string {
	return ExecuteCommand
}

// SEMCommand returns the command string for the statement.
func (n *ExplainForStmt) SEMCommand() string {
	return ExplainForConnectionCommand
}

// SEMCommand returns the command string for the statement.
func (n *ExplainStmt) SEMCommand() string {
	if n.Analyze {
		return ExplainAnalyzeCommand
	}
	return ExplainCommand
}

// SEMCommand returns the command string for the statement.
func (n *FlushStmt) SEMCommand() string {
	return FlushCommand
}

// SEMCommand returns the command string for the statement.
func (n *GrantStmt) SEMCommand() string {
	return GrantCommand
}

// SEMCommand returns the command string for the statement.
func (n *GrantProxyStmt) SEMCommand() string {
	return GrantProxyCommand
}

// SEMCommand returns the command string for the statement.
func (n *GrantRoleStmt) SEMCommand() string {
	return GrantRoleCommand
}

// SEMCommand returns the command string for the statement.
func (n *HelpStmt) SEMCommand() string {
	return HelpCommand
}

// SEMCommand returns the command string for the statement.
func (n *ImportIntoActionStmt) SEMCommand() string {
	return CancelImportIntoJobCommand
}

// SEMCommand returns the command string for the statement.
func (n *KillStmt) SEMCommand() string {
	return KillCommand
}

// SEMCommand returns the command string for the statement.
func (n *PlanReplayerStmt) SEMCommand() string {
	return PlanReplayerCommand
}

// SEMCommand returns the command string for the statement.
func (n *PrepareStmt) SEMCommand() string {
	return PrepareCommand
}

// SEMCommand returns the command string for the statement.
func (n *ReleaseSavepointStmt) SEMCommand() string {
	return ReleaseSavepointCommand
}

// SEMCommand returns the command string for the statement.
func (n *RestartStmt) SEMCommand() string {
	return RestartCommand
}

// SEMCommand returns the command string for the statement.
func (n *RevokeStmt) SEMCommand() string {
	return RevokeCommand
}

// SEMCommand returns the command string for the statement.
func (n *RevokeRoleStmt) SEMCommand() string {
	return RevokeRoleCommand
}

// SEMCommand returns the command string for the statement.
func (n *RollbackStmt) SEMCommand() string {
	return RollbackCommand
}

// SEMCommand returns the command string for the statement.
func (n *SavepointStmt) SEMCommand() string {
	return SavepointCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetBindingStmt) SEMCommand() string {
	return SetBindingCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetConfigStmt) SEMCommand() string {
	return SetConfigCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetDefaultRoleStmt) SEMCommand() string {
	return SetDefaultRoleCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetPwdStmt) SEMCommand() string {
	return SetPasswordCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetResourceGroupStmt) SEMCommand() string {
	return SetResourceGroupCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetRoleStmt) SEMCommand() string {
	return SetRoleCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetSessionStatesStmt) SEMCommand() string {
	return SetSessionStatesCommand
}

// SEMCommand returns the command string for the statement.
func (n *SetStmt) SEMCommand() string {
	return SetCommand
}

// SEMCommand returns the command string for the statement.
func (n *ShutdownStmt) SEMCommand() string {
	return ShutdownCommand
}

// SEMCommand returns the command string for the statement.
func (n *TraceStmt) SEMCommand() string {
	return TraceCommand
}

// SEMCommand returns the command string for the statement.
func (n *TrafficStmt) SEMCommand() string {
	return TrafficCommand
}

// SEMCommand returns the command string for the statement.
func (n *UseStmt) SEMCommand() string {
	return UseCommand
}

// SEMCommand returns the command string for the statement.
func (n *RecommendIndexStmt) SEMCommand() string {
	return RecommendIndexCommand
}

// Stats

// SEMCommand returns the command string for the statement.
func (n *LoadStatsStmt) SEMCommand() string {
	return LoadStatsCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropStatsStmt) SEMCommand() string {
	return DropStatsCommand
}

// SEMCommand returns the command string for the statement.
func (n *LockStatsStmt) SEMCommand() string {
	return LockStatsCommand
}

// SEMCommand returns the command string for the statement.
func (n *UnlockStatsStmt) SEMCommand() string {
	return UnlockStatsCommand
}

// SEMCommand returns the command string for the statement.
func (n *RefreshStatsStmt) SEMCommand() string {
	return RefreshStatsCommand
}

// Procedure Statements

// SEMCommand returns the command string for the statement.
func (n *ProcedureBlock) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureInfo) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *DropProcedureStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureIfInfo) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureElseIfBlock) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureElseBlock) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureIfBlock) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *SimpleWhenThenStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *SimpleCaseStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *SearchWhenThenStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *SearchCaseStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureRepeatStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureWhileStmt) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureOpenCur) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureCloseCur) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureFetchInto) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureLabelBlock) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureLabelLoop) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureJump) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureErrorCon) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureErrorVal) SEMCommand() string {
	return ProcedureCommand
}

// SEMCommand returns the command string for the statement.
func (n *ProcedureErrorState) SEMCommand() string {
	return ProcedureCommand
}
