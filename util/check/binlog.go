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
	"strings"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

// MySQLBinlogEnableChecker checks whether `log_bin` variable is enabled in MySQL.
type MySQLBinlogEnableChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogEnableChecker returns a Checker.
func NewMySQLBinlogEnableChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLBinlogEnableChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
func (pc *MySQLBinlogEnableChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog is enabled",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowLogBin(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "ON" {
		result.Errors = append(result.Errors, NewError("log_bin is %s, and should be ON", value))
		result.Instruction = "ref document: https://dev.mysql.com/doc/refman/5.7/en/replication-howto-masterbaseconfig.html"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLBinlogEnableChecker) Name() string {
	return "mysql_binlog_enable"
}

/*****************************************************/

// MySQLBinlogFormatChecker checks mysql binlog_format.
type MySQLBinlogFormatChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogFormatChecker returns a Checker.
func NewMySQLBinlogFormatChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLBinlogFormatChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
func (pc *MySQLBinlogFormatChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog_format is ROW",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowBinlogFormat(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "ROW" {
		result.Errors = append(result.Errors, NewError("binlog_format is %s, and should be ROW", value))
		result.Instruction = "please execute 'set global binlog_format=ROW;'"
		return result
	}
	result.State = StateSuccess

	return result
}

// Name implements the Checker interface.
func (pc *MySQLBinlogFormatChecker) Name() string {
	return "mysql_binlog_format"
}

/*****************************************************/

var (
	mysqlBinlogRowImageRequired   MySQLVersion = [3]uint{5, 6, 2}
	mariadbBinlogRowImageRequired MySQLVersion = [3]uint{10, 1, 6}
)

// MySQLBinlogRowImageChecker checks mysql binlog_row_image
type MySQLBinlogRowImageChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogRowImageChecker returns a Checker
func NewMySQLBinlogRowImageChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLBinlogRowImageChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// 'binlog_row_image' is introduced since mysql 5.6.2, and mariadb 10.1.6.
// > In MySQL 5.5 and earlier, full row images are always used for both before images and after images.
// So we need check 'binlog_row_image' after mysql 5.6.2 version and mariadb 10.1.6.
// ref:
// - https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_row_image
// - https://mariadb.com/kb/en/library/replication-and-binary-log-server-system-variables/#binlog_row_image
func (pc *MySQLBinlogRowImageChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog_row_image is FULL",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	// check version firstly
	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	// for mysql.version < 5.6.2 || mariadb.version < 10.1.6,  we don't need to check binlog_row_image.
	if (!IsMariaDB(value) && !version.Ge(mysqlBinlogRowImageRequired)) || (IsMariaDB(value) && !version.Ge(mariadbBinlogRowImageRequired)) {
		result.State = StateSuccess
		return result
	}

	value, err = dbutil.ShowBinlogRowImage(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "FULL" {
		result.Errors = append(result.Errors, NewError("binlog_row_image is %s, and should be FULL", value))
		result.Instruction = "please execute 'set global binlog_row_image = FULL;'"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLBinlogRowImageChecker) Name() string {
	return "mysql_binlog_row_image"
}
