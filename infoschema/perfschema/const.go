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

// perfSchemaTables is a shortcut to involve all table names.
var perfSchemaTables = []string{
	tableGlobalStatus,
	tableSessionStatus,
	tableSetupActors,
	tableSetupObjects,
	tableSetupInstruments,
	tableSetupConsumers,
	tableStmtsCurrent,
	tableStmtsHistory,
	tableStmtsHistoryLong,
	tablePreparedStmtsInstances,
	tableTransCurrent,
	tableTransHistory,
	tableTransHistoryLong,
	tableStagesCurrent,
	tableStagesHistory,
	tableStagesHistoryLong,
	tableEventsStatementsSummaryByDigest,
	tableEventsStatementsSummaryByDigestHistory,
	tableClusterEventsStatementsSummaryByDigest,
	tableClusterEventsStatementsSummaryByDigestHistory,
	tableTiDBProfileCPU,
	tableTiDBProfileMemory,
	tableTiDBProfileMutex,
	tableTiDBProfileAllocs,
	tableTiDBProfileBlock,
	tableTiDBProfileGoroutines,
	tableTiKVProfileCPU,
	tablePDProfileCPU,
	tablePDProfileMemory,
	tablePDProfileMutex,
	tablePDProfileAllocs,
	tablePDProfileBlock,
	tablePDProfileGoroutines,
}

// tableGlobalStatus contains the column name definitions for table global_status, same as MySQL.
const tableGlobalStatus = "CREATE TABLE performance_schema." + tableNameGlobalStatus + " (" +
	"VARIABLE_NAME VARCHAR(64) not null," +
	"VARIABLE_VALUE VARCHAR(1024));"

// tableSessionStatus contains the column name definitions for table session_status, same as MySQL.
const tableSessionStatus = "CREATE TABLE performance_schema." + tableNameSessionStatus + " (" +
	"VARIABLE_NAME VARCHAR(64) not null," +
	"VARIABLE_VALUE VARCHAR(1024));"

// tableSetupActors contains the column name definitions for table setup_actors, same as MySQL.
const tableSetupActors = "CREATE TABLE if not exists performance_schema." + tableNameSetupActors + " (" +
	"HOST			CHAR(60) NOT NULL  DEFAULT '%'," +
	"USER			CHAR(32) NOT NULL  DEFAULT '%'," +
	"ROLE			CHAR(16) NOT NULL  DEFAULT '%'," +
	"ENABLED		ENUM('YES','NO') NOT NULL  DEFAULT 'YES'," +
	"HISTORY		ENUM('YES','NO') NOT NULL  DEFAULT 'YES');"

// tableSetupObjects contains the column name definitions for table setup_objects, same as MySQL.
const tableSetupObjects = "CREATE TABLE if not exists performance_schema." + tableNameSetupObjects + " (" +
	"OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE') NOT NULL  DEFAULT 'TABLE'," +
	"OBJECT_SCHEMA		VARCHAR(64)  DEFAULT '%'," +
	"OBJECT_NAME		VARCHAR(64) NOT NULL  DEFAULT '%'," +
	"ENABLED		ENUM('YES','NO') NOT NULL  DEFAULT 'YES'," +
	"TIMED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');"

// tableSetupInstruments contains the column name definitions for table setup_instruments, same as MySQL.
const tableSetupInstruments = "CREATE TABLE if not exists performance_schema." + tableNameSetupInstruments + " (" +
	"NAME			VARCHAR(128) NOT NULL," +
	"ENABLED		ENUM('YES','NO') NOT NULL," +
	"TIMED			ENUM('YES','NO') NOT NULL);"

// tableSetupConsumers contains the column name definitions for table setup_consumers, same as MySQL.
const tableSetupConsumers = "CREATE TABLE if not exists performance_schema." + tableNameSetupConsumers + " (" +
	"NAME			VARCHAR(64) NOT NULL," +
	"ENABLED			ENUM('YES','NO') NOT NULL);"

// tableStmtsCurrent contains the column name definitions for table events_statements_current, same as MySQL.
const tableStmtsCurrent = "CREATE TABLE if not exists performance_schema." + tableNameEventsStatementsCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"SQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYSQL_ERRNO		INT(11)," +
	"RETURNED_SQLSTATE	VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// tableStmtsHistory contains the column name definitions for table events_statements_history, same as MySQL.
const tableStmtsHistory = "CREATE TABLE if not exists performance_schema." + tableNameEventsStatementsHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID		BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"SQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA 	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYSQL_ERRNO		INT(11)," +
	"RETURNED_SQLSTATE		VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// tableStmtsHistoryLong contains the column name definitions for table events_statements_history_long, same as MySQL.
const tableStmtsHistoryLong = "CREATE TABLE if not exists performance_schema." + tableNameEventsStatementsHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"SQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYSQL_ERRNO		INT(11)," +
	"RETURNED_SQLSTATE		VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// tablePreparedStmtsInstances contains the column name definitions for table prepared_statements_instances, same as MySQL.
const tablePreparedStmtsInstances = "CREATE TABLE if not exists performance_schema." + tableNamePreparedStatementsInstances + " (" +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED NOT NULL," +
	"STATEMENT_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"STATEMENT_NAME	VARCHAR(64)," +
	"SQL_TEXT		LONGTEXT NOT NULL," +
	"OWNER_THREAD_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"OWNER_EVENT_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"OWNER_OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE')," +
	"OWNER_OBJECT_SCHEMA		VARCHAR(64)," +
	"OWNER_OBJECT_NAME		VARCHAR(64)," +
	"TIMER_PREPARE	BIGINT(20) UNSIGNED NOT NULL," +
	"COUNT_REPREPARE	BIGINT(20) UNSIGNED NOT NULL," +
	"COUNT_EXECUTE	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"MIN_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_LOCK_TIME	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ERRORS		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_WARNINGS	BIGINT(20) UNSIGNED NOT NULL," +
	"		SUM_ROWS_AFFECTED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ROWS_SENT	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ROWS_EXAMINED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_CREATED_TMP_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_FULL_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_RANGE_CHECK	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_SCAN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_MERGE_PASSES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_ROWS	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_SCAN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_NO_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_NO_GOOD_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL);"

// tableTransCurrent contains the column name definitions for table events_transactions_current, same as MySQL.
const tableTransCurrent = "CREATE TABLE if not exists performance_schema." + tableNameEventsTransactionsCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// tableTransHistory contains the column name definitions for table events_transactions_history, same as MySQL.
//
const tableTransHistory = "CREATE TABLE if not exists performance_schema." + tableNameEventsTransactionsHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// tableTransHistoryLong contains the column name definitions for table events_transactions_history_long, same as MySQL.
const tableTransHistoryLong = "CREATE TABLE if not exists performance_schema." + tableNameEventsTransactionsHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// tableStagesCurrent contains the column name definitions for table events_stages_current, same as MySQL.
const tableStagesCurrent = "CREATE TABLE if not exists performance_schema." + tableNameEventsStagesCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// tableStagesHistory contains the column name definitions for table events_stages_history, same as MySQL.
const tableStagesHistory = "CREATE TABLE if not exists performance_schema." + tableNameEventsStagesHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// tableStagesHistoryLong contains the column name definitions for table events_stages_history_long, same as MySQL.
const tableStagesHistoryLong = "CREATE TABLE if not exists performance_schema." + tableNameEventsStagesHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// Fields in `events_statements_summary_by_digest` and `events_statements_summary_by_digest_history` are the same.
const fieldsInEventsStatementsSummary = "SUMMARY_BEGIN_TIME TIMESTAMP(6) NOT NULL," +
	"SUMMARY_END_TIME TIMESTAMP(6) NOT NULL," +
	"STMT_TYPE VARCHAR(64) NOT NULL," +
	"SCHEMA_NAME VARCHAR(64) DEFAULT NULL," +
	"DIGEST VARCHAR(64) NOT NULL," +
	"DIGEST_TEXT LONGTEXT NOT NULL," +
	"TABLE_NAMES TEXT DEFAULT NULL," +
	"INDEX_NAMES TEXT DEFAULT NULL," +
	"SAMPLE_USER VARCHAR(64) DEFAULT NULL," +
	"EXEC_COUNT BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"MIN_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_PARSE_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_PARSE_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_COMPILE_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COMPILE_LATENCY BIGINT(20) UNSIGNED NOT NULL," +
	"COP_TASK_NUM BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_COP_PROCESS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COP_PROCESS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COP_PROCESS_ADDRESS VARCHAR(256) DEFAULT NULL," +
	"AVG_COP_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COP_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COP_WAIT_ADDRESS VARCHAR(256) DEFAULT NULL," +
	"AVG_PROCESS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_PROCESS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_BACKOFF_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_BACKOFF_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_TOTAL_KEYS BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_TOTAL_KEYS BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_PROCESSED_KEYS BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_PROCESSED_KEYS BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_PREWRITE_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_PREWRITE_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_COMMIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COMMIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_GET_COMMIT_TS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_GET_COMMIT_TS_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_COMMIT_BACKOFF_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_COMMIT_BACKOFF_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_RESOLVE_LOCK_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_RESOLVE_LOCK_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_LOCAL_LATCH_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_LOCAL_LATCH_WAIT_TIME BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_WRITE_KEYS DOUBLE UNSIGNED NOT NULL," +
	"MAX_WRITE_KEYS BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_WRITE_SIZE DOUBLE NOT NULL," +
	"MAX_WRITE_SIZE BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_PREWRITE_REGIONS DOUBLE NOT NULL," +
	"MAX_PREWRITE_REGIONS INT(11) UNSIGNED NOT NULL," +
	"AVG_TXN_RETRY DOUBLE NOT NULL," +
	"MAX_TXN_RETRY INT(11) UNSIGNED NOT NULL," +
	"SUM_BACKOFF_TIMES BIGINT(20) UNSIGNED NOT NULL," +
	"BACKOFF_TYPES VARCHAR(1024) DEFAULT NULL," +
	"AVG_MEM BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_MEM BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_AFFECTED_ROWS DOUBLE UNSIGNED NOT NULL," +
	"FIRST_SEEN TIMESTAMP(6) NOT NULL," +
	"LAST_SEEN TIMESTAMP(6) NOT NULL," +
	"QUERY_SAMPLE_TEXT LONGTEXT DEFAULT NULL," +
	"PREV_SAMPLE_TEXT LONGTEXT DEFAULT NULL," +
	"PLAN_DIGEST VARCHAR(64) DEFAULT NULL," +
	"PLAN LONGTEXT DEFAULT NULL);"

// tableEventsStatementsSummaryByDigest contains the column name definitions for table
// events_statements_summary_by_digest, same as MySQL.
const tableEventsStatementsSummaryByDigest = "CREATE TABLE if not exists " + tableNameEventsStatementsSummaryByDigest +
	"(" + fieldsInEventsStatementsSummary

// tableEventsStatementsSummaryByDigestHistory contains the column name definitions for table
// events_statements_summary_by_digest_history.
const tableEventsStatementsSummaryByDigestHistory = "CREATE TABLE if not exists " + tableNameEventsStatementsSummaryByDigestHistory +
	"(" + fieldsInEventsStatementsSummary

// tableClusterEventsStatementsSummaryByDigest contains the column name definitions for table
// cluster_events_statements_summary_by_digest, same as MySQL.
const tableClusterEventsStatementsSummaryByDigest = "CREATE TABLE if not exists " + tableNameClusterEventsStatementsSummaryByDigest +
	"(ADDRESS VARCHAR(64) DEFAULT NULL," + fieldsInEventsStatementsSummary

// tableClusterEventsStatementsSummaryByDigestHistory contains the column name definitions for table
// cluster_events_statements_summary_by_digest_history.
const tableClusterEventsStatementsSummaryByDigestHistory = "CREATE TABLE if not exists " + tableNameClusterEventsStatementsSummaryByDigestHistory +
	"(ADDRESS VARCHAR(64) DEFAULT NULL," + fieldsInEventsStatementsSummary

// tableTiDBProfileCPU contains the columns name definitions for table tidb_profile_cpu
const tableTiDBProfileCPU = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileCPU + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tableTiDBProfileMemory contains the columns name definitions for table tidb_profile_memory
const tableTiDBProfileMemory = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileMemory + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tableTiDBProfileMutex contains the columns name definitions for table tidb_profile_mutex
const tableTiDBProfileMutex = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileMutex + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tableTiDBProfileAllocs contains the columns name definitions for table tidb_profile_allocs
const tableTiDBProfileAllocs = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileAllocs + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tableTiDBProfileBlock contains the columns name definitions for table tidb_profile_block
const tableTiDBProfileBlock = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileBlock + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tableTiDBProfileGoroutines contains the columns name definitions for table tidb_profile_goroutines
const tableTiDBProfileGoroutines = "CREATE TABLE IF NOT EXISTS " + tableNameTiDBProfileGoroutines + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"ID INT(8) NOT NULL," +
	"STATE VARCHAR(16) NOT NULL," +
	"LOCATION VARCHAR(512) NOT NULL);"

// tableTiKVProfileCPU contains the columns name definitions for table tikv_profile_cpu
const tableTiKVProfileCPU = "CREATE TABLE IF NOT EXISTS " + tableNameTiKVProfileCPU + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileCPU contains the columns name definitions for table pd_profile_cpu
const tablePDProfileCPU = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileCPU + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileMemory contains the columns name definitions for table pd_profile_cpu_memory
const tablePDProfileMemory = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileMemory + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileMutex contains the columns name definitions for table pd_profile_mutex
const tablePDProfileMutex = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileMutex + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileAllocs contains the columns name definitions for table pd_profile_allocs
const tablePDProfileAllocs = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileAllocs + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileBlock contains the columns name definitions for table pd_profile_block
const tablePDProfileBlock = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileBlock + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// tablePDProfileGoroutines contains the columns name definitions for table pd_profile_goroutines
const tablePDProfileGoroutines = "CREATE TABLE IF NOT EXISTS " + tableNamePDProfileGoroutines + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"ID INT(8) NOT NULL," +
	"STATE VARCHAR(16) NOT NULL," +
	"LOCATION VARCHAR(512) NOT NULL);"
