// Copyright 2016 PingCAP, Inc.
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

package perfschema

// Performance Schema Name.
const (
	Name = "PERFORMANCE_SCHEMA"
)

// Definition order same as MySQL's reference manual, so don't bother to
// adjust according to alphabetical order.
const (
	TableSetupActors            = "SETUP_ACTORS"
	TableSetupObjects           = "SETUP_OBJECTS"
	TableSetupInstruments       = "SETUP_INSTRUMENTS"
	TableSetupConsumers         = "SETUP_CONSUMERS"
	TableSetupTimers            = "SETUP_TIMERS"
	TableStmtsCurrent           = "EVENTS_STATEMENTS_CURRENT"
	TableStmtsHistory           = "EVENTS_STATEMENTS_HISTORY"
	TableStmtsHistoryLong       = "EVENTS_STATEMENTS_HISTORY_LONG"
	TablePreparedStmtsInstances = "PREPARED_STATEMENTS_INSTANCES"
	TableTransCurrent           = "EVENTS_TRANSACTIONS_CURRENT"
	TableTransHistory           = "EVENTS_TRANSACTIONS_HISTORY"
	TableTransHistoryLong       = "EVENTS_TRANSACTIONS_HISTORY_LONG"
	TableStagesCurrent          = "EVENTS_STAGES_CURRENT"
	TableStagesHistory          = "EVENTS_STAGES_HISTORY"
	TableStagesHistoryLong      = "EVENTS_STAGES_HISTORY_LONG"
)

// PerfSchemaTables is a shortcut to involve all table names.
var PerfSchemaTables = []string{
	TableSetupActors,
	TableSetupObjects,
	TableSetupInstruments,
	TableSetupConsumers,
	TableSetupTimers,
	TableStmtsCurrent,
	TableStmtsHistory,
	TableStmtsHistoryLong,
	TablePreparedStmtsInstances,
	TableTransCurrent,
	TableTransHistory,
	TableTransHistoryLong,
	TableStagesCurrent,
	TableStagesHistory,
	TableStagesHistoryLong,
}

// ColumnSetupActors contains the column name definitions for table setup_actors, same as MySQL.
var ColumnSetupActors = []string{"HOST", "USER", "ROLE", "ENABLED", "HISTORY"}

// ColumnSetupObjects contains the column name definitions for table setup_objects, same as MySQL.
var ColumnSetupObjects = []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"}

// ColumnSetupInstruments contains the column name definitions for table setup_instruments, same as MySQL.
var ColumnSetupInstruments = []string{"NAMED", "ENABLED", "TIMED"}

// ColumnSetupConsumers contains the column name definitions for table setup_consumers, same as MySQL.
var ColumnSetupConsumers = []string{"NAMED", "ENABLED"}

// ColumnSetupTimers contains the column name definitions for table setup_timers, same as MySQL.
var ColumnSetupTimers = []string{"NAME", "TIMER_NAME"}

// ColumnStmtsCurrent contains the column name definitions for table events_statements_current, same as MySQL.
var ColumnStmtsCurrent = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"LOCK_TIME",
	"SQL_TEXT",
	"DIGEST",
	"DIGEST_TEXT",
	"CURRENT_SCHEMA",
	"OBJECT_TYPE",
	"OBJECT_SCHEMA",
	"OBJECT_NAME",
	"OBJECT_INSTANCE_BEGIN",
	"MYSQL_ERRNO",
	"RETURNED_SQLSTATE",
	"MESSAGE_TEXT",
	"ERRORS",
	"WARNINGS",
	"ROWS_AFFECTED",
	"ROWS_SENT",
	"ROWS_EXAMINED",
	"CREATED_TMP_DISK_TABLES",
	"CREATED_TMP_TABLES",
	"SELECT_FULL_JOIN",
	"SELECT_FULL_RANGE_JOIN",
	"SELECT_RANGE",
	"SELECT_RANGE_CHECK",
	"SELECT_SCAN",
	"SORT_MERGE_PASSES",
	"SORT_RANGE",
	"SORT_ROWS",
	"SORT_SCAN",
	"NO_INDEX_USED",
	"NO_GOOD_INDEX_USED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
	"NESTING_EVENT_LEVEL",
}

// ColumnStmtsHistory contains the column name definitions for table events_statements_history, same as MySQL.
var ColumnStmtsHistory = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"LOCK_TIME",
	"SQL_TEXT",
	"DIGEST",
	"DIGEST_TEXT",
	"CURRENT_SCHEMA",
	"OBJECT_TYPE",
	"OBJECT_SCHEMA",
	"OBJECT_NAME",
	"OBJECT_INSTANCE_BEGIN",
	"MYSQL_ERRNO",
	"RETURNED_SQLSTATE",
	"MESSAGE_TEXT",
	"ERRORS",
	"WARNINGS",
	"ROWS_AFFECTED",
	"ROWS_SENT",
	"ROWS_EXAMINED",
	"CREATED_TMP_DISK_TABLES",
	"CREATED_TMP_TABLES",
	"SELECT_FULL_JOIN",
	"SELECT_FULL_RANGE_JOIN",
	"SELECT_RANGE",
	"SELECT_RANGE_CHECK",
	"SELECT_SCAN",
	"SORT_MERGE_PASSES",
	"SORT_RANGE",
	"SORT_ROWS",
	"SORT_SCAN",
	"NO_INDEX_USED",
	"NO_GOOD_INDEX_USED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
	"NESTING_EVENT_LEVEL",
}

// ColumnStmtsHistoryLong contains the column name definitions for table events_statements_history_long, same as MySQL.
var ColumnStmtsHistoryLong = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"LOCK_TIME",
	"SQL_TEXT",
	"DIGEST",
	"DIGEST_TEXT",
	"CURRENT_SCHEMA",
	"OBJECT_TYPE",
	"OBJECT_SCHEMA",
	"OBJECT_NAME",
	"OBJECT_INSTANCE_BEGIN",
	"MYSQL_ERRNO",
	"RETURNED_SQLSTATE",
	"MESSAGE_TEXT",
	"ERRORS",
	"WARNINGS",
	"ROWS_AFFECTED",
	"ROWS_SENT",
	"ROWS_EXAMINED",
	"CREATED_TMP_DISK_TABLES",
	"CREATED_TMP_TABLES",
	"SELECT_FULL_JOIN",
	"SELECT_FULL_RANGE_JOIN",
	"SELECT_RANGE",
	"SELECT_RANGE_CHECK",
	"SELECT_SCAN",
	"SORT_MERGE_PASSES",
	"SORT_RANGE",
	"SORT_ROWS",
	"SORT_SCAN",
	"NO_INDEX_USED",
	"NO_GOOD_INDEX_USED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
	"NESTING_EVENT_LEVEL",
}

// ColumnPreparedStmtsInstances contains the column name definitions for table prepared_statements_instances, same as MySQL.
var ColumnPreparedStmtsInstances = []string{
	"OBJECT_INSTANCE_BEGIN",
	"STATEMENT_ID",
	"STATEMENT_NAME",
	"SQL_TEXT",
	"OWNER_THREAD_ID",
	"OWNER_EVENT_ID",
	"OWNER_OBJECT_TYPE",
	"OWNER_OBJECT_SCHEMA",
	"OWNER_OBJECT_NAME",
	"TIMER_PREPARE",
	"COUNT_REPREPARE",
	"COUNT_EXECUTE",
	"SUM_TIMER_EXECUTE",
	"MIN_TIMER_EXECUTE",
	"AVG_TIMER_EXECUTE",
	"MAX_TIMER_EXECUTE",
	"SUM_LOCK_TIME",
	"SUM_ERRORS",
	"SUM_WARNINGS",
	"SUM_ROWS_AFFECTED",
	"SUM_ROWS_SENT",
	"SUM_ROWS_EXAMINED",
	"SUM_CREATED_TMP_DISK_TABLES",
	"SUM_CREATED_TMP_TABLES",
	"SUM_SELECT_FULL_JOIN",
	"SUM_SELECT_FULL_RANGE_JOIN",
	"SUM_SELECT_RANGE",
	"SUM_SELECT_RANGE_CHECK",
	"SUM_SELECT_SCAN",
	"SUM_SORT_MERGE_PASSES",
	"SUM_SORT_RANGE",
	"SUM_SORT_ROWS",
	"SUM_SORT_SCAN",
	"SUM_NO_INDEX_USED",
	"SUM_NO_GOOD_INDEX_USED",
}

// ColumnTransCurrent contains the column name definitions for table events_transactions_current, same as MySQL.
var ColumnTransCurrent = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"STATE",
	"TRX_ID",
	"GTID",
	"XID_FORMAT_ID",
	"XID_GTRID",
	"XID_BQUAL",
	"XA_STATE",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"ACCESS_MODE",
	"ISOLATION_LEVEL",
	"AUTOCOMMIT",
	"NUMBER_OF_SAVEPOINTS",
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT",
	"NUMBER_OF_RELEASE_SAVEPOINT",
	"OBJECT_INSTANCE_BEGIN",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}

// ColumnTransHistory contains the column name definitions for table events_transactions_history, same as MySQL.
var ColumnTransHistory = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"STATE",
	"TRX_ID",
	"GTID",
	"XID_FORMAT_ID",
	"XID_GTRID",
	"XID_BQUAL",
	"XA_STATE",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"ACCESS_MODE",
	"ISOLATION_LEVEL",
	"AUTOCOMMIT",
	"NUMBER_OF_SAVEPOINTS",
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT",
	"NUMBER_OF_RELEASE_SAVEPOINT",
	"OBJECT_INSTANCE_BEGIN",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}

// ColumnTransHistoryLong contains the column name definitions for table events_transactions_history_long, same as MySQL.
var ColumnTransHistoryLong = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"STATE",
	"TRX_ID",
	"GTID",
	"XID_FORMAT_ID",
	"XID_GTRID",
	"XID_BQUAL",
	"XA_STATE",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"ACCESS_MODE",
	"ISOLATION_LEVEL",
	"AUTOCOMMIT",
	"NUMBER_OF_SAVEPOINTS",
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT",
	"NUMBER_OF_RELEASE_SAVEPOINT",
	"OBJECT_INSTANCE_BEGIN",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}

// ColumnStagesCurrent contains the column name definitions for table events_stages_current, same as MySQL.
var ColumnStagesCurrent = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"WORK_COMPLETED",
	"WORK_ESTIMATED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}

// ColumnStagesHistory contains the column name definitions for table events_stages_history, same as MySQL.
var ColumnStagesHistory = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"WORK_COMPLETED",
	"WORK_ESTIMATED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}

// ColumnStagesHistoryLong contains the column name definitions for table events_stages_history_long, same as MySQL.
var ColumnStagesHistoryLong = []string{
	"THREAD_ID",
	"EVENT_ID",
	"END_EVENT_ID",
	"EVENT_NAME",
	"SOURCE",
	"TIMER_START",
	"TIMER_END",
	"TIMER_WAIT",
	"WORK_COMPLETED",
	"WORK_ESTIMATED",
	"NESTING_EVENT_ID",
	"NESTING_EVENT_TYPE",
}
