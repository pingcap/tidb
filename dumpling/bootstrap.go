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
	CreateUserTable = "CREATE TABLE if not exists mysql.user (Host CHAR(64), User CHAR(16), Password CHAR(41), PRIMARY KEY (Host, User));"
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
}

func initUserTable(s Session) {
	mustExecute(s, CreateUserTable)
	// Insert a default user with empty password.
	mustExecute(s, `INSERT INTO mysql.user VALUES ("localhost", "root", ""), ("127.0.0.1", "root", ""), ("::1", "root", "");`)
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}
