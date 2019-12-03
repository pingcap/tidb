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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

// Error instances.
var (
	ErrGetStartTS      = terror.ClassExecutor.New(mysql.ErrGetStartTS, mysql.MySQLErrName[mysql.ErrGetStartTS])
	ErrUnknownPlan     = terror.ClassExecutor.New(mysql.ErrUnknownPlan, mysql.MySQLErrName[mysql.ErrUnknownPlan])
	ErrPrepareMulti    = terror.ClassExecutor.New(mysql.ErrPrepareMulti, mysql.MySQLErrName[mysql.ErrPrepareMulti])
	ErrPrepareDDL      = terror.ClassExecutor.New(mysql.ErrPrepareDDL, mysql.MySQLErrName[mysql.ErrPrepareDDL])
	ErrResultIsEmpty   = terror.ClassExecutor.New(mysql.ErrResultIsEmpty, mysql.MySQLErrName[mysql.ErrResultIsEmpty])
	ErrBuildExecutor   = terror.ClassExecutor.New(mysql.ErrBuildExecutor, mysql.MySQLErrName[mysql.ErrBuildExecutor])
	ErrBatchInsertFail = terror.ClassExecutor.New(mysql.ErrBatchInsertFail, mysql.MySQLErrName[mysql.ErrBatchInsertFail])

	ErrCantCreateUserWithGrant     = terror.ClassExecutor.New(mysql.ErrCantCreateUserWithGrant, mysql.MySQLErrName[mysql.ErrCantCreateUserWithGrant])
	ErrPasswordNoMatch             = terror.ClassExecutor.New(mysql.ErrPasswordNoMatch, mysql.MySQLErrName[mysql.ErrPasswordNoMatch])
	ErrCannotUser                  = terror.ClassExecutor.New(mysql.ErrCannotUser, mysql.MySQLErrName[mysql.ErrCannotUser])
	ErrPasswordFormat              = terror.ClassExecutor.New(mysql.ErrPasswordFormat, mysql.MySQLErrName[mysql.ErrPasswordFormat])
	ErrCantChangeTxCharacteristics = terror.ClassExecutor.New(mysql.ErrCantChangeTxCharacteristics, mysql.MySQLErrName[mysql.ErrCantChangeTxCharacteristics])
	ErrPsManyParam                 = terror.ClassExecutor.New(mysql.ErrPsManyParam, mysql.MySQLErrName[mysql.ErrPsManyParam])
	ErrAdminCheckTable             = terror.ClassExecutor.New(mysql.ErrAdminCheckTable, mysql.MySQLErrName[mysql.ErrAdminCheckTable])
	ErrDBaccessDenied              = terror.ClassExecutor.New(mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied           = terror.ClassExecutor.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrBadDB                       = terror.ClassExecutor.New(mysql.ErrBadDB, mysql.MySQLErrName[mysql.ErrBadDB])
	ErrWrongObject                 = terror.ClassExecutor.New(mysql.ErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
	ErrRoleNotGranted              = terror.ClassPrivilege.New(mysql.ErrRoleNotGranted, mysql.MySQLErrName[mysql.ErrRoleNotGranted])
	ErrDeadlock                    = terror.ClassExecutor.New(mysql.ErrLockDeadlock, mysql.MySQLErrName[mysql.ErrLockDeadlock])
	ErrQueryInterrupted            = terror.ClassExecutor.New(mysql.ErrQueryInterrupted, mysql.MySQLErrName[mysql.ErrQueryInterrupted])
)

func init() {
	// Map error codes to mysql error codes.
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrGetStartTS:      mysql.ErrGetStartTS,
		mysql.ErrUnknownPlan:     mysql.ErrUnknownPlan,
		mysql.ErrPrepareMulti:    mysql.ErrPrepareMulti,
		mysql.ErrPrepareDDL:      mysql.ErrPrepareDDL,
		mysql.ErrResultIsEmpty:   mysql.ErrResultIsEmpty,
		mysql.ErrBuildExecutor:   mysql.ErrBuildExecutor,
		mysql.ErrBatchInsertFail: mysql.ErrBatchInsertFail,

		mysql.ErrCantCreateUserWithGrant:     mysql.ErrCantCreateUserWithGrant,
		mysql.ErrPasswordNoMatch:             mysql.ErrPasswordNoMatch,
		mysql.ErrCannotUser:                  mysql.ErrCannotUser,
		mysql.ErrPasswordFormat:              mysql.ErrPasswordFormat,
		mysql.ErrCantChangeTxCharacteristics: mysql.ErrCantChangeTxCharacteristics,
		mysql.ErrPsManyParam:                 mysql.ErrPsManyParam,
		mysql.ErrAdminCheckTable:             mysql.ErrAdminCheckTable,
		mysql.ErrDBaccessDenied:              mysql.ErrDBaccessDenied,
		mysql.ErrTableaccessDenied:           mysql.ErrTableaccessDenied,
		mysql.ErrBadDB:                       mysql.ErrBadDB,
		mysql.ErrWrongObject:                 mysql.ErrWrongObject,
		mysql.ErrRoleNotGranted:              mysql.ErrRoleNotGranted,
		mysql.ErrLockDeadlock:                mysql.ErrLockDeadlock,
		mysql.ErrQueryInterrupted:            mysql.ErrQueryInterrupted,
		mysql.ErrWrongValueCountOnRow:        mysql.ErrWrongValueCountOnRow,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExecutor] = tableMySQLErrCodes
}
