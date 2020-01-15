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
	pterror "github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Error instances.
var (
	ErrGetStartTS      = terror.New(pterror.ClassExecutor, mysql.ErrGetStartTS, mysql.MySQLErrName[mysql.ErrGetStartTS])
	ErrUnknownPlan     = terror.New(pterror.ClassExecutor, mysql.ErrUnknownPlan, mysql.MySQLErrName[mysql.ErrUnknownPlan])
	ErrPrepareMulti    = terror.New(pterror.ClassExecutor, mysql.ErrPrepareMulti, mysql.MySQLErrName[mysql.ErrPrepareMulti])
	ErrPrepareDDL      = terror.New(pterror.ClassExecutor, mysql.ErrPrepareDDL, mysql.MySQLErrName[mysql.ErrPrepareDDL])
	ErrResultIsEmpty   = terror.New(pterror.ClassExecutor, mysql.ErrResultIsEmpty, mysql.MySQLErrName[mysql.ErrResultIsEmpty])
	ErrBuildExecutor   = terror.New(pterror.ClassExecutor, mysql.ErrBuildExecutor, mysql.MySQLErrName[mysql.ErrBuildExecutor])
	ErrBatchInsertFail = terror.New(pterror.ClassExecutor, mysql.ErrBatchInsertFail, mysql.MySQLErrName[mysql.ErrBatchInsertFail])

	ErrCantCreateUserWithGrant     = terror.New(pterror.ClassExecutor, mysql.ErrCantCreateUserWithGrant, mysql.MySQLErrName[mysql.ErrCantCreateUserWithGrant])
	ErrPasswordNoMatch             = terror.New(pterror.ClassExecutor, mysql.ErrPasswordNoMatch, mysql.MySQLErrName[mysql.ErrPasswordNoMatch])
	ErrCannotUser                  = terror.New(pterror.ClassExecutor, mysql.ErrCannotUser, mysql.MySQLErrName[mysql.ErrCannotUser])
	ErrPasswordFormat              = terror.New(pterror.ClassExecutor, mysql.ErrPasswordFormat, mysql.MySQLErrName[mysql.ErrPasswordFormat])
	ErrCantChangeTxCharacteristics = terror.New(pterror.ClassExecutor, mysql.ErrCantChangeTxCharacteristics, mysql.MySQLErrName[mysql.ErrCantChangeTxCharacteristics])
	ErrPsManyParam                 = terror.New(pterror.ClassExecutor, mysql.ErrPsManyParam, mysql.MySQLErrName[mysql.ErrPsManyParam])
	ErrAdminCheckTable             = terror.New(pterror.ClassExecutor, mysql.ErrAdminCheckTable, mysql.MySQLErrName[mysql.ErrAdminCheckTable])
	ErrDBaccessDenied              = terror.New(pterror.ClassExecutor, mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied           = terror.New(pterror.ClassExecutor, mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrBadDB                       = terror.New(pterror.ClassExecutor, mysql.ErrBadDB, mysql.MySQLErrName[mysql.ErrBadDB])
	ErrWrongObject                 = terror.New(pterror.ClassExecutor, mysql.ErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
	ErrRoleNotGranted              = terror.New(pterror.ClassPrivilege, mysql.ErrRoleNotGranted, mysql.MySQLErrName[mysql.ErrRoleNotGranted])
	ErrDeadlock                    = terror.New(pterror.ClassExecutor, mysql.ErrLockDeadlock, mysql.MySQLErrName[mysql.ErrLockDeadlock])
	ErrQueryInterrupted            = terror.New(pterror.ClassExecutor, mysql.ErrQueryInterrupted, mysql.MySQLErrName[mysql.ErrQueryInterrupted])
)

func init() {
	// Map error codes to mysql error codes.
	tableMySQLErrCodes := map[pterror.ErrCode]uint16{
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
	terror.ErrClassToMySQLCodes[pterror.ClassExecutor] = tableMySQLErrCodes
}
