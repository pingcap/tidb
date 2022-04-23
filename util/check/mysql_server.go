// Copyright 2018 PingCAP, Inc.
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

package check

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersionChecker checks mysql/mariadb/rds,... version.
type MySQLVersionChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLVersionChecker returns a Checker
func NewMySQLVersionChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLVersionChecker{db: db, dbinfo: dbinfo}
}

// SupportedVersion defines the MySQL/MariaDB version that DM/syncer supports
// * 5.6.0 <= MySQL Version
// * 10.1.2 <= Mariadb Version
var SupportedVersion = map[string]struct {
	Min MySQLVersion
	Max MySQLVersion
}{
	"mysql": {
		MySQLVersion{5, 6, 0},
		MaxVersion,
	},
	"mariadb": {
		MySQLVersion{10, 1, 2},
		MaxVersion,
	},
}

// Check implements the Checker interface.
// we only support version >= 5.6
func (pc *MySQLVersionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql version is satisfied",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	pc.checkVersion(value, result)
	return result
}

func (pc *MySQLVersionChecker) checkVersion(value string, result *Result) {
	needVersion := SupportedVersion["mysql"]
	if IsMariaDB(value) {
		needVersion = SupportedVersion["mariadb"]
	}

	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return
	}

	if !version.Ge(needVersion.Min) {
		result.Errors = append(result.Errors, NewError("version required at least %v but got %v", needVersion.Min, version))
		result.Instruction = "Please upgrade your database system"
		return
	}

	if !version.Lt(needVersion.Max) {
		result.Errors = append(result.Errors, NewError("version required less than %v but got %v", needVersion.Max, version))
		return
	}

	result.State = StateSuccess
}

// Name implements the Checker interface.
func (pc *MySQLVersionChecker) Name() string {
	return "mysql_version"
}

/*****************************************************/

// MySQLServerIDChecker checks mysql/mariadb server ID.
type MySQLServerIDChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLServerIDChecker returns a Checker
func NewMySQLServerIDChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLServerIDChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
func (pc *MySQLServerIDChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql server_id has been greater than 0",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	serverID, err := dbutil.ShowServerID(ctx, pc.db)
	if err != nil {
		if utils.OriginError(err) == sql.ErrNoRows {
			result.Errors = append(result.Errors, NewError("server_id not set"))
			result.Instruction = "please set server_id in your database"
		} else {
			markCheckError(result, err)
		}

		return result
	}

	if serverID == 0 {
		result.Errors = append(result.Errors, NewError("server_id is 0"))
		result.Instruction = "please set server_id greater than 0"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLServerIDChecker) Name() string {
	return "mysql_server_id"
}
