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

//! `pkg/meta/metadef/system.go`: reserved global-ID bounds and the fixed
//! table IDs of TiDB's system tables (extracted by script from the Go
//! constants for fidelity).

/// Go `ReservedGlobalIDUpperBound`: the max value of any physical schema
/// object ID. The first 2 bytes were once planned for multi-tenancy.
pub const RESERVED_GLOBAL_ID_UPPER_BOUND: i64 = 0x0000_FFFF_FFFF_FFFF;
/// Go `ReservedGlobalIDLowerBound`: reserves 1000 IDs; the usable range for
/// user schema objects is `[1, RESERVED_GLOBAL_ID_LOWER_BOUND]`.
pub const RESERVED_GLOBAL_ID_LOWER_BOUND: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 1000;
/// Go `MaxUserGlobalID`: the max user schema object ID, inclusive.
pub const MAX_USER_GLOBAL_ID: i64 = RESERVED_GLOBAL_ID_LOWER_BOUND;

/// Go `SystemDatabaseID`.
pub const SYSTEM_DATABASE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND;
/// Go `TiDBDDLJobTableID`.
pub const TI_DBDDLJOB_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 1;
/// Go `TiDBDDLReorgTableID`.
pub const TI_DBDDLREORG_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 2;
/// Go `TiDBDDLHistoryTableID`.
pub const TI_DBDDLHISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 3;
/// Go `TiDBMDLInfoTableID`.
pub const TI_DBMDLINFO_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 4;
/// Go `TiDBBackgroundSubtaskTableID`.
pub const TI_DBBACKGROUND_SUBTASK_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 5;
/// Go `TiDBBackgroundSubtaskHistoryTableID`.
pub const TI_DBBACKGROUND_SUBTASK_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 6;
/// Go `TiDBDDLNotifierTableID`.
pub const TI_DBDDLNOTIFIER_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 7;
/// Go `UserTableID`.
pub const USER_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 8;
/// Go `PasswordHistoryTableID`.
pub const PASSWORD_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 9;
/// Go `GlobalPrivTableID`.
pub const GLOBAL_PRIV_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 10;
/// Go `DBTableID`.
pub const DBTABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 11;
/// Go `TablesPrivTableID`.
pub const TABLES_PRIV_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 12;
/// Go `ColumnsPrivTableID`.
pub const COLUMNS_PRIV_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 13;
/// Go `GlobalVariablesTableID`.
pub const GLOBAL_VARIABLES_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 14;
/// Go `TiDBTableID`.
pub const TI_DBTABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 15;
/// Go `HelpTopicTableID`.
pub const HELP_TOPIC_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 16;
/// Go `StatsMetaTableID`.
pub const STATS_META_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 17;
/// Go `StatsHistogramsTableID`.
pub const STATS_HISTOGRAMS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 18;
/// Go `StatsBucketsTableID`.
pub const STATS_BUCKETS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 19;
/// Go `GCDeleteRangeTableID`.
pub const GCDELETE_RANGE_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 20;
/// Go `GCDeleteRangeDoneTableID`.
pub const GCDELETE_RANGE_DONE_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 21;
/// Go `StatsFeedbackTableID`.
pub const STATS_FEEDBACK_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 22;
/// Go `RoleEdgesTableID`.
pub const ROLE_EDGES_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 23;
/// Go `DefaultRolesTableID`.
pub const DEFAULT_ROLES_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 24;
/// Go `BindInfoTableID`.
pub const BIND_INFO_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 25;
/// Go `StatsTopNTableID`.
pub const STATS_TOP_NTABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 26;
/// Go `ExprPushdownBlacklistTableID`.
pub const EXPR_PUSHDOWN_BLACKLIST_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 27;
/// Go `OptRuleBlacklistTableID`.
pub const OPT_RULE_BLACKLIST_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 28;
/// Go `StatsExtendedTableID`.
pub const STATS_EXTENDED_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 29;
/// Go `StatsFMSketchTableID`.
pub const STATS_FMSKETCH_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 30;
/// Go `GlobalGrantsTableID`.
pub const GLOBAL_GRANTS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 31;
/// Go `CapturePlanBaselinesBlacklistTableID`.
pub const CAPTURE_PLAN_BASELINES_BLACKLIST_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 32;
/// Go `ColumnStatsUsageTableID`.
pub const COLUMN_STATS_USAGE_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 33;
/// Go `TableCacheMetaTableID`.
pub const TABLE_CACHE_META_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 34;
/// Go `AnalyzeOptionsTableID`.
pub const ANALYZE_OPTIONS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 35;
/// Go `StatsHistoryTableID`.
pub const STATS_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 36;
/// Go `StatsMetaHistoryTableID`.
pub const STATS_META_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 37;
/// Go `AnalyzeJobsTableID`.
pub const ANALYZE_JOBS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 38;
/// Go `AdvisoryLocksTableID`.
pub const ADVISORY_LOCKS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 39;
/// Go `PlanReplayerStatusTableID`.
pub const PLAN_REPLAYER_STATUS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 40;
/// Go `PlanReplayerTaskTableID`.
pub const PLAN_REPLAYER_TASK_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 41;
/// Go `StatsTableLockedTableID`.
pub const STATS_TABLE_LOCKED_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 42;
/// Go `TiDBTTLTableStatusTableID`.
pub const TI_DBTTLTABLE_STATUS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 43;
/// Go `TiDBTTLTaskTableID`.
pub const TI_DBTTLTASK_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 44;
/// Go `TiDBTTLJobHistoryTableID`.
pub const TI_DBTTLJOB_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 45;
/// Go `TiDBGlobalTaskTableID`.
pub const TI_DBGLOBAL_TASK_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 46;
/// Go `TiDBGlobalTaskHistoryTableID`.
pub const TI_DBGLOBAL_TASK_HISTORY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 47;
/// Go `TiDBImportJobsTableID`.
pub const TI_DBIMPORT_JOBS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 48;
/// Go `TiDBRunawayWatchTableID`.
pub const TI_DBRUNAWAY_WATCH_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 49;
/// Go `TiDBRunawayQueriesTableID`.
pub const TI_DBRUNAWAY_QUERIES_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 50;
/// Go `TiDBTimersTableID`.
pub const TI_DBTIMERS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 51;
/// Go `TiDBRunawayWatchDoneTableID`.
pub const TI_DBRUNAWAY_WATCH_DONE_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 52;
/// Go `DistFrameworkMetaTableID`.
pub const DIST_FRAMEWORK_META_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 53;
/// Go `RequestUnitByGroupTableID`.
pub const REQUEST_UNIT_BY_GROUP_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 54;
/// Go `TiDBPITRIDMapTableID`.
pub const TI_DBPITRIDMAP_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 55;
/// Go `TiDBRestoreRegistryTableID`.
pub const TI_DBRESTORE_REGISTRY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 56;
/// Go `IndexAdvisorResultsTableID`.
pub const INDEX_ADVISOR_RESULTS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 57;
/// Go `TiDBKernelOptionsTableID`.
pub const TI_DBKERNEL_OPTIONS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 58;
/// Go `TiDBWorkloadValuesTableID`.
pub const TI_DBWORKLOAD_VALUES_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 59;
/// Go `SysDatabaseID`.
pub const SYS_DATABASE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 60;
/// Go `TiDBSoftDeleteTableStatusTableID`.
pub const TI_DBSOFT_DELETE_TABLE_STATUS_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 61;
/// Go `TiDBMaskingPolicyTableID`.
pub const TI_DBMASKING_POLICY_TABLE_ID: i64 = RESERVED_GLOBAL_ID_UPPER_BOUND - 62;

/// Go `IsReservedID`: whether `id` is a reserved global ID.
#[must_use]
pub fn is_reserved_id(id: i64) -> bool {
    RESERVED_GLOBAL_ID_LOWER_BOUND < id && id <= RESERVED_GLOBAL_ID_UPPER_BOUND
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestIsReservedID.
    #[test]
    fn is_reserved_id_cases() {
        assert!(is_reserved_id(RESERVED_GLOBAL_ID_UPPER_BOUND));
        assert!(is_reserved_id(RESERVED_GLOBAL_ID_LOWER_BOUND + 1));
        assert!(!is_reserved_id(RESERVED_GLOBAL_ID_LOWER_BOUND));
        assert!(!is_reserved_id(123));
    }
}
