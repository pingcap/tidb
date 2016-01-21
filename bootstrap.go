// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package tidb

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host			CHAR(64),
		User			CHAR(16),
		Password		CHAR(41),
		Select_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Insert_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Update_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Delete_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Drop_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Grant_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Alter_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Show_db_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Execute_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Index_priv		ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_user_priv	ENUM('N','Y') NOT NULL  DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE if not exists mysql.db (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Select_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Insert_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Update_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Delete_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Create_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Drop_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Grant_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Index_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Alter_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		Execute_priv	ENUM('N','Y') Not Null  DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index','Alter'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(16),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGloablVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGloablVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE if not exists mysql.tidb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`
)

const (
	// CreateSetupActorsTable is the SQL statement creates setup_actors table in perf db.
	CreateSetupActorsTable = `CREATE TABLE if not exists performance_schema.setup_actors (
		HOST			CHAR(60) NOT NULL  DEFAULT '%',
		USER			CHAR(32) NOT NULL  DEFAULT '%',
		ROLE			CHAR(16) NOT NULL  DEFAULT '%',
		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
		HISTORY			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');`
	// CreateSetupObjectsTable is the SQL statement creates setup_objects table in perf db.
	CreateSetupObjectsTable = `CREATE TABLE if not exists performance_schema.setup_objects (
		OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE') NOT NULL  DEFAULT 'TABLE',
		OBJECT_SCHEMA	VARCHAR(64)  DEFAULT '%',
		OBJECT_NAME		VARCHAR(64) NOT NULL  DEFAULT '%',
		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
		TIMED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');`
	// CreateSetupInstrsTable is the SQL statement creates setup_instruments table in perf db.
	CreateSetupInstrsTable = `CREATE TABLE if not exists performance_schema.setup_instruments (
		NAME			VARCHAR(128) NOT NULL,
		ENABLED			ENUM('YES','NO') NOT NULL,
		TIMED			ENUM('YES','NO') NOT NULL);`
	// CreateSetupConsumersTable is the SQL statement creates setup_consumers table in perf db.
	CreateSetupConsumersTable = `CREATE TABLE if not exists performance_schema.setup_consumers (
		NAME			VARCHAR(64) NOT NULL,
		ENABLED			ENUM('YES','NO') NOT NULL);`
	// CreateSetupTimersTable is the SQL statement creates setup_timers table in perf db.
	CreateSetupTimersTable = `CREATE TABLE if not exists performance_schema.setup_timers (
		NAME			VARCHAR(64) NOT NULL,
		TIMER_NAME		ENUM('NANOSECOND','MICROSECOND','MILLISECOND') NOT NULL);`
)

const (
	// CreateStatementsCurrentTable is the SQL statement creates events_statements_current table in perf db.
	CreateStatementsCurrentTable = `CREATE TABLE if not exists performance_schema.events_statements_current (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
		SQL_TEXT		LONGTEXT,
		DIGEST			VARCHAR(32),
		DIGEST_TEXT		LONGTEXT,
		CURRENT_SCHEMA	VARCHAR(64),
		OBJECT_TYPE		VARCHAR(64),
		OBJECT_SCHEMA	VARCHAR(64),
		OBJECT_NAME		VARCHAR(64),
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		MYSQL_ERRNO		INT(11),
		RETURNED_SQLSTATE		VARCHAR(5),
		MESSAGE_TEXT	VARCHAR(128),
		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
		NESTING_EVENT_LEVEL		INT(11));`
	// CreateStatementsHistoryTable is the SQL statement creates events_statements_history table in perf db.
	CreateStatementsHistoryTable = `CREATE TABLE if not exists performance_schema.events_statements_history (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
		SQL_TEXT		LONGTEXT,
		DIGEST			VARCHAR(32),
		DIGEST_TEXT		LONGTEXT,
		CURRENT_SCHEMA	VARCHAR(64),
		OBJECT_TYPE		VARCHAR(64),
		OBJECT_SCHEMA	VARCHAR(64),
		OBJECT_NAME		VARCHAR(64),
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		MYSQL_ERRNO		INT(11),
		RETURNED_SQLSTATE		VARCHAR(5),
		MESSAGE_TEXT	VARCHAR(128),
		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
		NESTING_EVENT_LEVEL		INT(11));`
	// CreateStatementsHistoryLongTable is the SQL statement creates events_statements_history_long table in perf db.
	CreateStatementsHistoryLongTable = `CREATE TABLE if not exists performance_schema.events_statements_history_long (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
		SQL_TEXT		LONGTEXT,
		DIGEST			VARCHAR(32),
		DIGEST_TEXT		LONGTEXT,
		CURRENT_SCHEMA	VARCHAR(64),
		OBJECT_TYPE		VARCHAR(64),
		OBJECT_SCHEMA	VARCHAR(64),
		OBJECT_NAME		VARCHAR(64),
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		MYSQL_ERRNO		INT(11),
		RETURNED_SQLSTATE		VARCHAR(5),
		MESSAGE_TEXT	VARCHAR(128),
		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
		NESTING_EVENT_LEVEL		INT(11));`
	// CreatePreparedStatementsInstancesTable is the SQL statement creates prepared_statements_instances table in perf db.
	CreatePreparedStatementsInstancesTable = `CREATE TABLE if not exists performance_schema.prepared_statements_instances (
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED NOT NULL,
		STATEMENT_ID	BIGINT(20) UNSIGNED NOT NULL,
		STATEMENT_NAME	VARCHAR(64),
		SQL_TEXT		LONGTEXT NOT NULL,
		OWNER_THREAD_ID	BIGINT(20) UNSIGNED NOT NULL,
		OWNER_EVENT_ID	BIGINT(20) UNSIGNED NOT NULL,
		OWNER_OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE'),
		OWNER_OBJECT_SCHEMA		VARCHAR(64),
		OWNER_OBJECT_NAME		VARCHAR(64),
		TIMER_PREPARE	BIGINT(20) UNSIGNED NOT NULL,
		COUNT_REPREPARE	BIGINT(20) UNSIGNED NOT NULL,
		COUNT_EXECUTE	BIGINT(20) UNSIGNED NOT NULL,
		SUM_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
		MIN_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
		AVG_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
		MAX_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
		SUM_LOCK_TIME	BIGINT(20) UNSIGNED NOT NULL,
		SUM_ERRORS		BIGINT(20) UNSIGNED NOT NULL,
		SUM_WARNINGS	BIGINT(20) UNSIGNED NOT NULL,
		SUM_ROWS_AFFECTED		BIGINT(20) UNSIGNED NOT NULL,
		SUM_ROWS_SENT	BIGINT(20) UNSIGNED NOT NULL,
		SUM_ROWS_EXAMINED		BIGINT(20) UNSIGNED NOT NULL,
		SUM_CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
		SUM_CREATED_TMP_TABLES	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SELECT_FULL_JOIN	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SELECT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
		SUM_SELECT_RANGE_CHECK	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SELECT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SORT_MERGE_PASSES	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SORT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SORT_ROWS	BIGINT(20) UNSIGNED NOT NULL,
		SUM_SORT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
		SUM_NO_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
		SUM_NO_GOOD_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL);`
)

const (
	// CreateTransactionsCurrentTable is the SQL statement creates events_transactions_current table in perf db.
	CreateTransactionsCurrentTable = `CREATE TABLE if not exists performance_schema.events_transactions_current (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
		TRX_ID			BIGINT(20) UNSIGNED,
		GTID			VARCHAR(64),
		XID_FORMAT_ID	INT(11),
		XID_GTRID		VARCHAR(130),
		XID_BQUAL		VARCHAR(130),
		XA_STATE		VARCHAR(64),
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
		ISOLATION_LEVEL	VARCHAR(64),
		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
	// CreateTransactionsHistoryTable is the SQL statement creates events_transactions_history table in perf db.
	CreateTransactionsHistoryTable = `CREATE TABLE if not exists performance_schema.events_transactions_history (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
		TRX_ID			BIGINT(20) UNSIGNED,
		GTID			VARCHAR(64),
		XID_FORMAT_ID	INT(11),
		XID_GTRID		VARCHAR(130),
		XID_BQUAL		VARCHAR(130),
		XA_STATE		VARCHAR(64),
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
		ISOLATION_LEVEL	VARCHAR(64),
		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
	// CreateTransactionsHistoryLongTable is the SQL statement creates events_transactions_history_long table in perf db.
	CreateTransactionsHistoryLongTable = `CREATE TABLE if not exists performance_schema.events_transactions_history_long (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
		TRX_ID			BIGINT(20) UNSIGNED,
		GTID			VARCHAR(64),
		XID_FORMAT_ID	INT(11),
		XID_GTRID		VARCHAR(130),
		XID_BQUAL		VARCHAR(130),
		XA_STATE		VARCHAR(64),
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
		ISOLATION_LEVEL	VARCHAR(64),
		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
)

const (
	// CreateStagesCurrentTable is the SQL statement creates events_stages_current table in perf db.
	CreateStagesCurrentTable = `CREATE TABLE if not exists performance_schema.events_stages_current (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		WORK_COMPLETED	BIGINT(20) UNSIGNED,
		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
	// CreateStagesHistoryTable is the SQL statement creates events_stages_history table in perf db.
	CreateStagesHistoryTable = `CREATE TABLE if not exists performance_schema.events_stages_history (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		WORK_COMPLETED	BIGINT(20) UNSIGNED,
		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
	// CreateStagesHistoryLongTable is the SQL statement creates events_stages_history_long table in perf db.
	CreateStagesHistoryLongTable = `CREATE TABLE if not exists performance_schema.events_stages_history_long (
		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
		END_EVENT_ID	BIGINT(20) UNSIGNED,
		EVENT_NAME		VARCHAR(128) NOT NULL,
		SOURCE			VARCHAR(64),
		TIMER_START		BIGINT(20) UNSIGNED,
		TIMER_END		BIGINT(20) UNSIGNED,
		TIMER_WAIT		BIGINT(20) UNSIGNED,
		WORK_COMPLETED	BIGINT(20) UNSIGNED,
		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));`
)

// Bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	b, err := checkBootstrapped(s)
	if err != nil {
		log.Fatal(err)
	}
	if b {
		return
	}
	doDDLWorks(s)
	doDMLWorks(s)
}

const (
	bootstrappedVar     = "bootstrapped"
	bootstrappedVarTrue = "True"
)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.Execute(fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err != nil && infoschema.DatabaseNotExists.NotEqual(err) {
		log.Fatal(err)
	}
	// Check bootstrapped variable value in TiDB table.
	v, err := checkBootstrappedVar(s)
	if err != nil {
		return false, errors.Trace(err)
	}
	return v, nil
}

func checkBootstrappedVar(s Session) (bool, error) {
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar)
	rs, err := s.Execute(sql)
	if err != nil {
		if infoschema.TableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	if len(rs) != 1 {
		return false, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	row, err := r.Next()
	if err != nil || row == nil {
		return false, errors.Trace(err)
	}

	isBootstrapped := row.Data[0].(string) == bootstrappedVarTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.

		if err = s.FinishTxn(false); err != nil {
			return false, errors.Trace(err)
		}
	}

	return isBootstrapped, nil
}

// Execute DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege tables.
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global systemt variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create perf db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.PerfSchemaDB))
	// Create setup_actors table.
	mustExecute(s, CreateSetupActorsTable)
	// Create setup_objects table.
	mustExecute(s, CreateSetupObjectsTable)
	// Create setup_instruments table.
	mustExecute(s, CreateSetupInstrsTable)
	// Create setup_consumers table.
	mustExecute(s, CreateSetupConsumersTable)
	// Create setup_timers table.
	mustExecute(s, CreateSetupTimersTable)
	// Create events_statements_current table.
	mustExecute(s, CreateStatementsCurrentTable)
	// Create events_statements_history table.
	mustExecute(s, CreateStatementsHistoryTable)
	// Create events_statements_history_long table.
	mustExecute(s, CreateStatementsHistoryLongTable)
	// Create prepared_statements_instances table.
	mustExecute(s, CreatePreparedStatementsInstancesTable)
	// Create events_transactions_current table.
	mustExecute(s, CreateTransactionsCurrentTable)
	// Create events_transactions_history table.
	mustExecute(s, CreateTransactionsHistoryTable)
	// Create events_transactions_history_long table.
	mustExecute(s, CreateTransactionsHistoryLongTable)
	// Create events_stages_current table.
	mustExecute(s, CreateStagesCurrentTable)
	// Create events_stages_history table.
	mustExecute(s, CreateStagesHistoryTable)
	// Create events_stages_history_long table.
	mustExecute(s, CreateStagesHistoryLongTable)
}

// Execute DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, bootstrappedVarTrue, bootstrappedVarTrue)
	mustExecute(s, sql)
	mustExecute(s, "COMMIT")
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}
