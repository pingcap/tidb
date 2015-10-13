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

	"github.com/ngaut/log"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/errors"
	"github.com/pingcap/tidb/util/errors2"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host CHAR(64), 
		User CHAR(16), 
		Password CHAR(41), 
		Select_priv  ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Insert_priv  ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Update_priv  ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Delete_priv  ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_priv  ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Drop_priv    ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Grant_priv   ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Alter_priv   ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Show_db_priv ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Execute_priv ENUM('N','Y') NOT NULL  DEFAULT 'N',
		Create_user_priv ENUM('N','Y') NOT NULL  DEFAULT 'N',
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
)

// Bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")

	//  Check if system db exists.
	_, err := s.Execute(fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err == nil {
		// We have already finished bootstrap.
		return
	} else if !errors2.ErrorEqual(err, errors.ErrDatabaseNotExist) {
		log.Fatal(err)
	}
	mustExecute(s, fmt.Sprintf("CREATE DATABASE %s;", mysql.SystemDB))
	initUserTable(s)
	initPrivTables(s)
}

func initUserTable(s Session) {
	mustExecute(s, CreateUserTable)
	// Insert a default user with empty password.
	mustExecute(s, `INSERT INTO mysql.user VALUES ("localhost", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y"), 
		("127.0.0.1", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y"), 
		("::1", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y");`)
}

// Initiates privilege tables including mysql.db, mysql.tables_priv and mysql.column_priv.
func initPrivTables(s Session) {
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}
