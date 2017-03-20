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
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
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

	// CreateStatsMetaTable store's the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version)
	);`
)

// Bootstrap initiates system DB for a store.
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
	version2 = 2
	version3 = 3
	version4 = 4
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

// Get variable value from mysql.tidb table.
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
	row, err := r.Next()
	if err != nil || row == nil {
		return types.Datum{}, errors.Trace(err)
	}
	return row.Data[0], nil
}

// When the system is boostrapped by low version TiDB server, we should do some upgrade works.
// For example, add new system variables into mysql.global_variables table.
func upgrade(s Session) {
	ver, err := getBootstrapVersion(s)
	if err != nil {
		log.Fatal(errors.Trace(err))
	}
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		if err1 := s.CommitTxn(); err1 != nil {
			// Make sure that doesn't affect the following operations.
			log.Fatal(errors.Trace(err1))
		}
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
			if err1 := s.CommitTxn(); err1 != nil {
				// Make sure that doesn't affect the following operations.
				log.Fatal(errors.Trace(err1))
			}
			return
		}
		log.Errorf("[Upgrade] upgrade from %d to %d error", ver, currentBootstrapVersion)
		log.Fatal(err)
	}
	return
}

// Update to version 2.
func upgradeToVer2(s Session) {
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.DistSQLScanConcurrencyVar, variable.DistSQLJoinConcurrencyVar}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT IGNORE INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// Update to version 3.
func upgradeToVer3(s Session) {
	// Version 3 fix tx_read_only variable value.
	sql := fmt.Sprintf("UPDATE %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		mysql.SystemDB, mysql.GlobalVariablesTable)
	mustExecute(s, sql)
}

// Update to version 4.
func upgradeToVer4(s Session) {
	sql := CreateStatsMetaTable
	mustExecute(s, sql)
}

// Update boostrap version variable in mysql.TiDB table.
func updateBootstrapVer(s Session) {
	// Update bootstrap version.
	sql := fmt.Sprintf(`INSERT INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, sql)
}

// Gets bootstrap version from mysql.tidb table;
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
	// Create global system variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
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
