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
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host				CHAR(64),
		User				CHAR(16),
		Password			CHAR(41),
		Select_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE if not exists mysql.db (
		Host			CHAR(60),
		DB				CHAR(64),
		User			CHAR(16),
		Select_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Insert_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Update_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Delete_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Drop_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Grant_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Index_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Alter_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Execute_priv	ENUM('N','Y') Not Null DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB			CHAR(64),
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
		DB			CHAR(64),
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

	// CreateHelpTopic is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopic = `CREATE TABLE if not exists mysql.help_topic (
  		help_topic_id int(10) unsigned NOT NULL,
  		name char(64) NOT NULL,
  		help_category_id smallint(5) unsigned NOT NULL,
  		description text NOT NULL,
  		example text NOT NULL,
  		url text NOT NULL,
  		PRIMARY KEY (help_topic_id),
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE if not exists mysql.stats_histograms (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		distinct_count bigint(64) NOT NULL,
		null_count bigint(64) NOT NULL DEFAULT 0,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) unsigned NOT NULL DEFAULT 0,
		unique index tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists mysql.stats_buckets (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		bucket_id bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		repeats bigint(64) NOT NULL,
		upper_bound blob NOT NULL,
		lower_bound blob ,
		unique index tbl(table_id, is_index, hist_id, bucket_id)
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	b, err := checkBootstrapped(s)
	if err != nil {
		log.Fatal(err)
	}
	if b {
		upgrade(s)
	}
	doDDLWorks(s)
	doDMLWorks(s)
}

const (
	// The variable name in mysql.TiDB table.
	// It is used for checking if the store is boostrapped by any TiDB server.
	bootstrappedVar = "bootstrapped"
	// The variable value in mysql.TiDB table for bootstrappedVar.
	// If the value true, the store is already boostrapped by a TiDB server.
	bootstrappedVarTrue = "True"
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB server which bootstrapped the store.
	tidbServerVersionVar = "tidb_server_version" //
	// Const for TiDB server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.Execute(fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err != nil && infoschema.ErrDatabaseNotExists.NotEqual(err) {
		log.Fatal(err)
	}
	// Check bootstrapped variable value in TiDB table.
	d, err := getTiDBVar(s, bootstrappedVar)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := d.GetString() == bootstrappedVarTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s Session, name string) (types.Datum, error) {
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, name)
	rs, err := s.Execute(sql)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if len(rs) != 1 {
		return types.Datum{}, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	defer r.Close()
	row, err := r.Next()
	if err != nil || row == nil {
		return types.Datum{}, errors.Trace(err)
	}
	return row.Data[0], nil
}

// upgrade function  will do some upgrade works, when the system is boostrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s Session) {
	ver, err := getBootstrapVersion(s)
	if err != nil {
		log.Fatal(errors.Trace(err))
	}
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}
	// Do upgrade works then update bootstrap version.
	if ver < version2 {
		upgradeToVer2(s)
		ver = version2
	}
	if ver < version3 {
		upgradeToVer3(s)
	}
	if ver < version4 {
		upgradeToVer4(s)
	}

	if ver < version5 {
		upgradeToVer5(s)
	}

	if ver < version6 {
		upgradeToVer6(s)
	}

	if ver < version7 {
		upgradeToVer7(s)
	}

	if ver < version8 {
		upgradeToVer8(s)
	}

	if ver < version9 {
		upgradeToVer9(s)
	}

	if ver < version10 {
		upgradeToVer10(s)
	}

	if ver < version11 {
		upgradeToVer11(s)
	}

	if ver < version12 {
		upgradeToVer12(s)
	}

	if ver < version13 {
		upgradeToVer13(s)
	}

	updateBootstrapVer(s)
	_, err = s.Execute("COMMIT")

	if err != nil {
		time.Sleep(1 * time.Second)
		// Check if TiDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			log.Fatal(err1)
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version TiDB server.
			return
		}
		log.Errorf("[Upgrade] upgrade from %d to %d error", ver, currentBootstrapVersion)
		log.Fatal(err)
	}
	return
}

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s Session) {
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.TiDBDistSQLScanConcurrency}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT IGNORE INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// upgradeToVer3 updates to version 3.
func upgradeToVer3(s Session) {
	// Version 3 fix tx_read_only variable value.
	sql := fmt.Sprintf("UPDATE %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		mysql.SystemDB, mysql.GlobalVariablesTable)
	mustExecute(s, sql)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s Session) {
	sql := CreateStatsMetaTable
	mustExecute(s, sql)
}

func upgradeToVer5(s Session) {
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s Session) {
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.Execute("SELECT `Process_priv` from mysql.user limit 0"); err == nil {
		return
	}
	upgradeToVer7(s)
}

func upgradeToVer9(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s Session, sql string, ignorableErrs ...error) {
	_, err := s.Execute(sql)
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}

func upgradeToVer10(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", ddl.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", ddl.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s Session) {
	_, err := s.Execute("ALTER TABLE mysql.user ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
			return
		}
		log.Fatal(err)
	}
	mustExecute(s, "UPDATE mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s Session) {
	s.Execute("BEGIN")
	sql := "SELECT user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.Execute(sql)
	if err != nil {
		log.Fatal(err)
		return
	}
	r := rs[0]
	sqls := make([]string, 0, 1)
	defer r.Close()
	row, err := r.Next()
	for err == nil && row != nil {
		user := row.Data[0].GetString()
		host := row.Data[1].GetString()
		pass := row.Data[2].GetString()
		newpass, err := oldPasswordUpgrade(pass)
		if err != nil {
			log.Fatal(err)
			return
		}
		sql := fmt.Sprintf(`UPDATE mysql.user set password = "%s" where user="%s" and host="%s"`, newpass, user, host)
		sqls = append(sqls, sql)
		row, err = r.Next()
	}

	if err != nil {
		log.Fatal(err)
		return
	}

	for _, sql := range sqls {
		mustExecute(s, sql)
	}

	sql = fmt.Sprintf(`INSERT INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, version12, version12)
	mustExecute(s, sql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s Session) {
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	for _, sql := range sqls {
		_, err := s.Execute(sql)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				continue
			}
			log.Fatal(err)
		}
	}
	mustExecute(s, "UPDATE mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_view_priv='Y',Show_view_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y'")
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s Session) {
	// Update bootstrap version.
	sql := fmt.Sprintf(`INSERT INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, sql)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s Session) (int64, error) {
	d, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if d.IsNull() {
		return 0, nil
	}
	return strconv.ParseInt(d.GetString(), 10, 64)
}

// doDDLWorks executes DDL statements in bootstrap stage.
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
	// Create global system variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// Session only variable should not be inserted.
		if v.Scope != variable.ScopeSession {
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, bootstrappedVarTrue, bootstrappedVarTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion)
	mustExecute(s, sql)

	_, err := s.Execute("COMMIT")
	if err != nil {
		time.Sleep(1 * time.Second)
		// Check if TiDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			log.Fatal(err1)
		}
		if b {
			return
		}
		log.Fatal(err)
	}
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}

// oldPasswordUpgrade upgrade password to MySQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		return "", errors.Trace(err)
	}

	hash2 := util.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}
