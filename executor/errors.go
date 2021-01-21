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
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

// Error instances.
var (
	ErrGetStartTS           = dbterror.ClassExecutor.NewStd(mysql.ErrGetStartTS)
	ErrUnknownPlan          = dbterror.ClassExecutor.NewStd(mysql.ErrUnknownPlan)
	ErrPrepareMulti         = dbterror.ClassExecutor.NewStd(mysql.ErrPrepareMulti)
	ErrPrepareDDL           = dbterror.ClassExecutor.NewStd(mysql.ErrPrepareDDL)
	ErrResultIsEmpty        = dbterror.ClassExecutor.NewStd(mysql.ErrResultIsEmpty)
	ErrBuildExecutor        = dbterror.ClassExecutor.NewStd(mysql.ErrBuildExecutor)
	ErrBatchInsertFail      = dbterror.ClassExecutor.NewStd(mysql.ErrBatchInsertFail)
	ErrUnsupportedPs        = dbterror.ClassExecutor.NewStd(mysql.ErrUnsupportedPs)
	ErrSubqueryMoreThan1Row = dbterror.ClassExecutor.NewStd(mysql.ErrSubqueryNo1Row)

	ErrCantCreateUserWithGrant     = dbterror.ClassExecutor.NewStd(mysql.ErrCantCreateUserWithGrant)
	ErrPasswordNoMatch             = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordNoMatch)
	ErrCannotUser                  = dbterror.ClassExecutor.NewStd(mysql.ErrCannotUser)
	ErrPasswordFormat              = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordFormat)
	ErrCantChangeTxCharacteristics = dbterror.ClassExecutor.NewStd(mysql.ErrCantChangeTxCharacteristics)
	ErrPsManyParam                 = dbterror.ClassExecutor.NewStd(mysql.ErrPsManyParam)
	ErrAdminCheckTable             = dbterror.ClassExecutor.NewStd(mysql.ErrAdminCheckTable)
	ErrDBaccessDenied              = dbterror.ClassExecutor.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied           = dbterror.ClassExecutor.NewStd(mysql.ErrTableaccessDenied)
	ErrBadDB                       = dbterror.ClassExecutor.NewStd(mysql.ErrBadDB)
	ErrWrongObject                 = dbterror.ClassExecutor.NewStd(mysql.ErrWrongObject)
	ErrRoleNotGranted              = dbterror.ClassPrivilege.NewStd(mysql.ErrRoleNotGranted)
	ErrDeadlock                    = dbterror.ClassExecutor.NewStd(mysql.ErrLockDeadlock)
	ErrQueryInterrupted            = dbterror.ClassExecutor.NewStd(mysql.ErrQueryInterrupted)

	ErrBRIEBackupFailed  = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEBackupFailed)
	ErrBRIERestoreFailed = dbterror.ClassExecutor.NewStd(mysql.ErrBRIERestoreFailed)
	ErrBRIEImportFailed  = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEImportFailed)
	ErrBRIEExportFailed  = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEExportFailed)
)
