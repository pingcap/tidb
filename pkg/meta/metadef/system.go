// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadef

const (
	// ReservedGlobalIDUpperBound is the max value of any physical schema object ID.
	// due to history reasons, the first 2 bytes are planned to be used for multi
	// tenancy, but it's replaced by keyspace.
	ReservedGlobalIDUpperBound int64 = 0x0000FFFFFFFFFFFF
	// ReservedGlobalIDLowerBound reserves 1000 IDs.
	// valid usable ID range for user schema objects is [1, ReservedGlobalIDLowerBound].
	//
	// (ReservedGlobalIDLowerBound, ReservedGlobalIDUpperBound] is reserved for
	// system schema objects.
	ReservedGlobalIDLowerBound = ReservedGlobalIDUpperBound - 1000
	// MaxUserGlobalID is the max value of user schema object ID, inclusive.
	MaxUserGlobalID = ReservedGlobalIDLowerBound
)

const (
	// SystemDatabaseID is the database ID of `mysql`.
	SystemDatabaseID = ReservedGlobalIDUpperBound
	// TiDBDDLJobTableID is the table ID of `tidb_ddl_job`.
	TiDBDDLJobTableID = ReservedGlobalIDUpperBound - 1
	// TiDBDDLReorgTableID is the table ID of `tidb_ddl_reorg`.
	TiDBDDLReorgTableID = ReservedGlobalIDUpperBound - 2
	// TiDBDDLHistoryTableID is the table ID of `tidb_ddl_history`.
	TiDBDDLHistoryTableID = ReservedGlobalIDUpperBound - 3
	// TiDBMDLInfoTableID is the table ID of `tidb_mdl_info`.
	TiDBMDLInfoTableID = ReservedGlobalIDUpperBound - 4
	// TiDBBackgroundSubtaskTableID is the table ID of `tidb_background_subtask`.
	TiDBBackgroundSubtaskTableID = ReservedGlobalIDUpperBound - 5
	// TiDBBackgroundSubtaskHistoryTableID is the table ID of `tidb_background_subtask_history`.
	TiDBBackgroundSubtaskHistoryTableID = ReservedGlobalIDUpperBound - 6
	// TiDBDDLNotifierTableID is the table ID of `tidb_ddl_notifier`.
	TiDBDDLNotifierTableID = ReservedGlobalIDUpperBound - 7
	// UserTableID is the table ID of `user`.
	UserTableID = ReservedGlobalIDUpperBound - 8
	// PasswordHistoryTableID is the table ID of `password_history`.
	PasswordHistoryTableID = ReservedGlobalIDUpperBound - 9
	// GlobalPrivTableID is the table ID of `global_priv`.
	GlobalPrivTableID = ReservedGlobalIDUpperBound - 10
	// DBTableID is the table ID of `db`.
	DBTableID = ReservedGlobalIDUpperBound - 11
	// TablesPrivTableID is the table ID of `table_priv`.
	TablesPrivTableID = ReservedGlobalIDUpperBound - 12
	// ColumnsPrivTableID is the table ID of `column_priv`.
	ColumnsPrivTableID = ReservedGlobalIDUpperBound - 13
	// GlobalVariablesTableID is the table ID of `global_variables`.
	GlobalVariablesTableID = ReservedGlobalIDUpperBound - 14
	// TiDBTableID is the table ID of `tidb`.
	TiDBTableID = ReservedGlobalIDUpperBound - 15
	// HelpTopicTableID is the table ID of `help_topic`.
	HelpTopicTableID = ReservedGlobalIDUpperBound - 16
	// StatsMetaTableID is the table ID of `stats_meta`.
	StatsMetaTableID = ReservedGlobalIDUpperBound - 17
	// StatsHistogramsTableID is the table ID of `stats_histograms`.
	StatsHistogramsTableID = ReservedGlobalIDUpperBound - 18
	// StatsBucketsTableID is the table ID of `stats_buckets`.
	StatsBucketsTableID = ReservedGlobalIDUpperBound - 19
	// GCDeleteRangeTableID is the table ID of `gc_delete_range`.
	GCDeleteRangeTableID = ReservedGlobalIDUpperBound - 20
	// GCDeleteRangeDoneTableID is the table ID of `gc_delete_range_done`.
	GCDeleteRangeDoneTableID = ReservedGlobalIDUpperBound - 21
	// StatsFeedbackTableID is the table ID of `stats_feedback`.
	StatsFeedbackTableID = ReservedGlobalIDUpperBound - 22
	// RoleEdgesTableID is the table ID of `role_edges`.
	RoleEdgesTableID = ReservedGlobalIDUpperBound - 23
	// DefaultRolesTableID is the table ID of `default_roles`.
	DefaultRolesTableID = ReservedGlobalIDUpperBound - 24
	// BindInfoTableID is the table ID of `bind_info`.
	BindInfoTableID = ReservedGlobalIDUpperBound - 25
	// StatsTopNTableID is the table ID of `stats_top_n`.
	StatsTopNTableID = ReservedGlobalIDUpperBound - 26
	// ExprPushdownBlacklistTableID is the table ID of `expr_pushdown_blacklist`.
	ExprPushdownBlacklistTableID = ReservedGlobalIDUpperBound - 27
	// OptRuleBlacklistTableID is the table ID of `opt_rule_blacklist`.
	OptRuleBlacklistTableID = ReservedGlobalIDUpperBound - 28
	// StatsExtendedTableID is the table ID of `stats_extended`.
	StatsExtendedTableID = ReservedGlobalIDUpperBound - 29
	// StatsFMSketchTableID is the table ID of `stats_fm_sketch`.
	StatsFMSketchTableID = ReservedGlobalIDUpperBound - 30
	// GlobalGrantsTableID is the table ID of `global_grants`.
	GlobalGrantsTableID = ReservedGlobalIDUpperBound - 31
	// CapturePlanBaselinesBlacklistTableID is the table ID of `capture_plan_baselines_blacklist`.
	CapturePlanBaselinesBlacklistTableID = ReservedGlobalIDUpperBound - 32
	// ColumnStatsUsageTableID is the table ID of `column_stats_usage`.
	ColumnStatsUsageTableID = ReservedGlobalIDUpperBound - 33
	// TableCacheMetaTableID is the table ID of `table_cache_meta`.
	TableCacheMetaTableID = ReservedGlobalIDUpperBound - 34
	// AnalyzeOptionsTableID is the table ID of `analyze_options`.
	AnalyzeOptionsTableID = ReservedGlobalIDUpperBound - 35
	// StatsHistoryTableID is the table ID of `stats_history`.
	StatsHistoryTableID = ReservedGlobalIDUpperBound - 36
	// StatsMetaHistoryTableID is the table ID of `stats_meta_history`.
	StatsMetaHistoryTableID = ReservedGlobalIDUpperBound - 37
	// AnalyzeJobsTableID is the table ID of `analyze_jobs`.
	AnalyzeJobsTableID = ReservedGlobalIDUpperBound - 38
	// AdvisoryLocksTableID is the table ID of `advisory_locks`.
	AdvisoryLocksTableID = ReservedGlobalIDUpperBound - 39
	// PlanReplayerStatusTableID is the table ID of `plan_replayer_status`.
	PlanReplayerStatusTableID = ReservedGlobalIDUpperBound - 40
	// PlanReplayerTaskTableID is the table ID of `plan_replayer_task`.
	PlanReplayerTaskTableID = ReservedGlobalIDUpperBound - 41
	// StatsTableLockedTableID is the table ID of `stats_table_locked`.
	StatsTableLockedTableID = ReservedGlobalIDUpperBound - 42
	// TiDBTTLTableStatusTableID is the table ID of `tidb_ttl_table_status`.
	TiDBTTLTableStatusTableID = ReservedGlobalIDUpperBound - 43
	// TiDBTTLTaskTableID is the table ID of `tidb_ttl_task`.
	TiDBTTLTaskTableID = ReservedGlobalIDUpperBound - 44
	// TiDBTTLJobHistoryTableID is the table ID of `tidb_ttl_job_history`.
	TiDBTTLJobHistoryTableID = ReservedGlobalIDUpperBound - 45
	// TiDBGlobalTaskTableID is the table ID of `tidb_global_task`.
	TiDBGlobalTaskTableID = ReservedGlobalIDUpperBound - 46
	// TiDBGlobalTaskHistoryTableID is the table ID of `tidb_global_task_history`.
	TiDBGlobalTaskHistoryTableID = ReservedGlobalIDUpperBound - 47
	// TiDBImportJobsTableID is the table ID of `tidb_import_jobs`.
	TiDBImportJobsTableID = ReservedGlobalIDUpperBound - 48
	// TiDBRunawayWatchTableID is the table ID of `tidb_runaway_watch`.
	TiDBRunawayWatchTableID = ReservedGlobalIDUpperBound - 49
	// TiDBRunawayQueriesTableID is the table ID of `tidb_runaway`.
	TiDBRunawayQueriesTableID = ReservedGlobalIDUpperBound - 50
	// TiDBTimersTableID is the table ID of `tidb_timers`.
	TiDBTimersTableID = ReservedGlobalIDUpperBound - 51
	// TiDBRunawayWatchDoneTableID is the table ID of `tidb_done_runaway_watch`.
	TiDBRunawayWatchDoneTableID = ReservedGlobalIDUpperBound - 52
	// DistFrameworkMetaTableID is the table ID of `dist_framework_meta`.
	DistFrameworkMetaTableID = ReservedGlobalIDUpperBound - 53
	// RequestUnitByGroupTableID is the table ID of `request_unit_by_group`.
	RequestUnitByGroupTableID = ReservedGlobalIDUpperBound - 54
	// TiDBPITRIDMapTableID is the table ID of `tidb_pitr_id_map`.
	TiDBPITRIDMapTableID = ReservedGlobalIDUpperBound - 55
	// TiDBRestoreRegistryTableID is the table ID of `tidb_restore_registry`.
	TiDBRestoreRegistryTableID = ReservedGlobalIDUpperBound - 56
	// IndexAdvisorResultsTableID is the table ID of `index_advisor`.
	IndexAdvisorResultsTableID = ReservedGlobalIDUpperBound - 57
	// TiDBKernelOptionsTableID is the table ID of `tidb_kernel_options`.
	TiDBKernelOptionsTableID = ReservedGlobalIDUpperBound - 58
	// TiDBWorkloadValuesTableID is the table ID of `tidb_workload_values`.
	TiDBWorkloadValuesTableID = ReservedGlobalIDUpperBound - 59
	// SysDatabaseID is the database ID of `sys`.
	SysDatabaseID = ReservedGlobalIDUpperBound - 60
)

// IsReservedID checks if the given ID is a reserved global ID.
func IsReservedID(id int64) bool {
	return ReservedGlobalIDLowerBound < id && id <= ReservedGlobalIDUpperBound
}
