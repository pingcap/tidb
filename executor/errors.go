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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

// Error instances.
var (
	ErrGetStartTS      = terror.ClassExecutor.NewStd(mysql.ErrGetStartTS)
	ErrUnknownPlan     = terror.ClassExecutor.NewStd(mysql.ErrUnknownPlan)
	ErrPrepareMulti    = terror.ClassExecutor.NewStd(mysql.ErrPrepareMulti)
	ErrPrepareDDL      = terror.ClassExecutor.NewStd(mysql.ErrPrepareDDL)
	ErrResultIsEmpty   = terror.ClassExecutor.NewStd(mysql.ErrResultIsEmpty)
	ErrBuildExecutor   = terror.ClassExecutor.NewStd(mysql.ErrBuildExecutor)
	ErrBatchInsertFail = terror.ClassExecutor.NewStd(mysql.ErrBatchInsertFail)

	ErrCantCreateUserWithGrant     = terror.ClassExecutor.NewStd(mysql.ErrCantCreateUserWithGrant)
	ErrPasswordNoMatch             = terror.ClassExecutor.NewStd(mysql.ErrPasswordNoMatch)
	ErrCannotUser                  = terror.ClassExecutor.NewStd(mysql.ErrCannotUser)
	ErrPasswordFormat              = terror.ClassExecutor.NewStd(mysql.ErrPasswordFormat)
	ErrCantChangeTxCharacteristics = terror.ClassExecutor.NewStd(mysql.ErrCantChangeTxCharacteristics)
	ErrPsManyParam                 = terror.ClassExecutor.NewStd(mysql.ErrPsManyParam)
	ErrAdminCheckTable             = terror.ClassExecutor.NewStd(mysql.ErrAdminCheckTable)
	ErrDBaccessDenied              = terror.ClassExecutor.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied           = terror.ClassExecutor.NewStd(mysql.ErrTableaccessDenied)
	ErrBadDB                       = terror.ClassExecutor.NewStd(mysql.ErrBadDB)
	ErrWrongObject                 = terror.ClassExecutor.NewStd(mysql.ErrWrongObject)
	ErrRoleNotGranted              = terror.ClassPrivilege.NewStd(mysql.ErrRoleNotGranted)
	ErrDeadlock                    = terror.ClassExecutor.NewStd(mysql.ErrLockDeadlock)
	ErrQueryInterrupted            = terror.ClassExecutor.NewStd(mysql.ErrQueryInterrupted)

	ErrBRIEBackupFailed  = terror.ClassExecutor.NewStd(mysql.ErrBRIEBackupFailed)
	ErrBRIERestoreFailed = terror.ClassExecutor.NewStd(mysql.ErrBRIERestoreFailed)
	ErrBRIEImportFailed  = terror.ClassExecutor.NewStd(mysql.ErrBRIEImportFailed)
	ErrBRIEExportFailed  = terror.ClassExecutor.NewStd(mysql.ErrBRIEExportFailed)
)
