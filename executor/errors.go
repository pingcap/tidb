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

package executor

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Error codes that are not mapping to mysql error codes.
const (
	codeUnknownPlan = iota
	codePrepareMulti
	codePrepareDDL
	codeResultIsEmpty
	codeErrBuildExec
	codeBatchInsertFail
)

// Error instances.
var (
	ErrUnknownPlan     = terror.ClassExecutor.New(codeUnknownPlan, "Unknown plan")
	ErrPrepareMulti    = terror.ClassExecutor.New(codePrepareMulti, "Can not prepare multiple statements")
	ErrPrepareDDL      = terror.ClassExecutor.New(codePrepareDDL, "Can not prepare DDL statements")
	ErrResultIsEmpty   = terror.ClassExecutor.New(codeResultIsEmpty, "result is empty")
	ErrBuildExecutor   = terror.ClassExecutor.New(codeErrBuildExec, "Failed to build executor")
	ErrBatchInsertFail = terror.ClassExecutor.New(codeBatchInsertFail, "Batch insert failed, please clean the table and try again.")

	ErrPasswordNoMatch             = terror.ClassExecutor.New(mysql.ErrPasswordNoMatch, mysql.MySQLErrName[mysql.ErrPasswordNoMatch])
	ErrCannotUser                  = terror.ClassExecutor.New(mysql.ErrCannotUser, mysql.MySQLErrName[mysql.ErrCannotUser])
	ErrPasswordFormat              = terror.ClassExecutor.New(mysql.ErrPasswordFormat, mysql.MySQLErrName[mysql.ErrPasswordFormat])
	ErrCantChangeTxCharacteristics = terror.ClassExecutor.New(mysql.ErrCantChangeTxCharacteristics, mysql.MySQLErrName[mysql.ErrCantChangeTxCharacteristics])
	ErrPsManyParam                 = terror.ClassExecutor.New(mysql.ErrPsManyParam, mysql.MySQLErrName[mysql.ErrPsManyParam])
	ErrAdminCheckTable             = terror.ClassExecutor.New(mysql.ErrAdminCheckTable, mysql.MySQLErrName[mysql.ErrAdminCheckTable])
)

func init() {
	// Map error codes to mysql error codes.
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrPasswordNoMatch:             mysql.ErrPasswordNoMatch,
		mysql.ErrCannotUser:                  mysql.ErrCannotUser,
		mysql.ErrWrongValueCountOnRow:        mysql.ErrWrongValueCountOnRow,
		mysql.ErrPasswordFormat:              mysql.ErrPasswordFormat,
		mysql.ErrCantChangeTxCharacteristics: mysql.ErrCantChangeTxCharacteristics,
		mysql.ErrPsManyParam:                 mysql.ErrPsManyParam,
		mysql.ErrAdminCheckTable:             mysql.ErrAdminCheckTable,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExecutor] = tableMySQLErrCodes
}
