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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exeerrors

import (
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// Error instances.
var (
	ErrGetStartTS           = dbterror.ClassExecutor.NewStd(errno.ErrGetStartTS)
	ErrUnknownPlan          = dbterror.ClassExecutor.NewStd(errno.ErrUnknownPlan)
	ErrPrepareMulti         = dbterror.ClassExecutor.NewStd(errno.ErrPrepareMulti)
	ErrPrepareDDL           = dbterror.ClassExecutor.NewStd(errno.ErrPrepareDDL)
	ErrResultIsEmpty        = dbterror.ClassExecutor.NewStd(errno.ErrResultIsEmpty)
	ErrBuildExecutor        = dbterror.ClassExecutor.NewStd(errno.ErrBuildExecutor)
	ErrBatchInsertFail      = dbterror.ClassExecutor.NewStd(errno.ErrBatchInsertFail)
	ErrUnsupportedPs        = dbterror.ClassExecutor.NewStd(errno.ErrUnsupportedPs)
	ErrSubqueryMoreThan1Row = dbterror.ClassExecutor.NewStd(errno.ErrSubqueryNo1Row)
	ErrIllegalGrantForTable = dbterror.ClassExecutor.NewStd(errno.ErrIllegalGrantForTable)
	ErrColumnsNotMatched    = dbterror.ClassExecutor.NewStd(errno.ErrColumnNotMatched)

	ErrCantCreateUserWithGrant              = dbterror.ClassExecutor.NewStd(errno.ErrCantCreateUserWithGrant)
	ErrPasswordNoMatch                      = dbterror.ClassExecutor.NewStd(errno.ErrPasswordNoMatch)
	ErrCannotUser                           = dbterror.ClassExecutor.NewStd(errno.ErrCannotUser)
	ErrGrantRole                            = dbterror.ClassExecutor.NewStd(errno.ErrGrantRole)
	ErrPasswordFormat                       = dbterror.ClassExecutor.NewStd(errno.ErrPasswordFormat)
	ErrCantChangeTxCharacteristics          = dbterror.ClassExecutor.NewStd(errno.ErrCantChangeTxCharacteristics)
	ErrPsManyParam                          = dbterror.ClassExecutor.NewStd(errno.ErrPsManyParam)
	ErrAdminCheckTable                      = dbterror.ClassExecutor.NewStd(errno.ErrAdminCheckTable)
	ErrDBaccessDenied                       = dbterror.ClassExecutor.NewStd(errno.ErrDBaccessDenied)
	ErrTableaccessDenied                    = dbterror.ClassExecutor.NewStd(errno.ErrTableaccessDenied)
	ErrBadDB                                = dbterror.ClassExecutor.NewStd(errno.ErrBadDB)
	ErrWrongObject                          = dbterror.ClassExecutor.NewStd(errno.ErrWrongObject)
	ErrWrongUsage                           = dbterror.ClassExecutor.NewStd(errno.ErrWrongUsage)
	ErrRoleNotGranted                       = dbterror.ClassPrivilege.NewStd(errno.ErrRoleNotGranted)
	ErrDeadlock                             = dbterror.ClassExecutor.NewStd(errno.ErrLockDeadlock)
	ErrQueryInterrupted                     = dbterror.ClassExecutor.NewStd(errno.ErrQueryInterrupted)
	ErrMaxExecTimeExceeded                  = dbterror.ClassExecutor.NewStd(errno.ErrMaxExecTimeExceeded)
	ErrResourceGroupQueryRunawayInterrupted = dbterror.ClassExecutor.NewStd(errno.ErrResourceGroupQueryRunawayInterrupted)
	ErrResourceGroupQueryRunawayQuarantine  = dbterror.ClassExecutor.NewStd(errno.ErrResourceGroupQueryRunawayQuarantine)
	ErrDynamicPrivilegeNotRegistered        = dbterror.ClassExecutor.NewStd(errno.ErrDynamicPrivilegeNotRegistered)
	ErrIllegalPrivilegeLevel                = dbterror.ClassExecutor.NewStd(errno.ErrIllegalPrivilegeLevel)
	ErrInvalidSplitRegionRanges             = dbterror.ClassExecutor.NewStd(errno.ErrInvalidSplitRegionRanges)
	ErrViewInvalid                          = dbterror.ClassExecutor.NewStd(errno.ErrViewInvalid)
	ErrInstanceScope                        = dbterror.ClassExecutor.NewStd(errno.ErrInstanceScope)
	ErrSettingNoopVariable                  = dbterror.ClassExecutor.NewStd(errno.ErrSettingNoopVariable)
	ErrLazyUniquenessCheckFailure           = dbterror.ClassExecutor.NewStd(errno.ErrLazyUniquenessCheckFailure)
	ErrMemoryExceedForQuery                 = dbterror.ClassExecutor.NewStd(errno.ErrMemoryExceedForQuery)
	ErrMemoryExceedForInstance              = dbterror.ClassExecutor.NewStd(errno.ErrMemoryExceedForInstance)
	ErrDeleteNotFoundColumn                 = dbterror.ClassExecutor.NewStd(errno.ErrDeleteNotFoundColumn)

	ErrBRIEBackupFailed               = dbterror.ClassExecutor.NewStd(errno.ErrBRIEBackupFailed)
	ErrBRIERestoreFailed              = dbterror.ClassExecutor.NewStd(errno.ErrBRIERestoreFailed)
	ErrBRIEImportFailed               = dbterror.ClassExecutor.NewStd(errno.ErrBRIEImportFailed)
	ErrBRIEExportFailed               = dbterror.ClassExecutor.NewStd(errno.ErrBRIEExportFailed)
	ErrBRJobNotFound                  = dbterror.ClassExecutor.NewStd(errno.ErrBRJobNotFound)
	ErrCTEMaxRecursionDepth           = dbterror.ClassExecutor.NewStd(errno.ErrCTEMaxRecursionDepth)
	ErrPluginIsNotLoaded              = dbterror.ClassExecutor.NewStd(errno.ErrPluginIsNotLoaded)
	ErrSetPasswordAuthPlugin          = dbterror.ClassExecutor.NewStd(errno.ErrSetPasswordAuthPlugin)
	ErrFuncNotEnabled                 = dbterror.ClassExecutor.NewStdErr(errno.ErrNotSupportedYet, errno.Message("%-.32s is not supported. To enable this experimental feature, set '%-.32s' in the configuration file.", nil))
	ErrSavepointNotExists             = dbterror.ClassExecutor.NewStd(errno.ErrSpDoesNotExist)
	ErrForeignKeyCascadeDepthExceeded = dbterror.ClassExecutor.NewStd(errno.ErrForeignKeyCascadeDepthExceeded)
	ErrPasswordExpireAnonymousUser    = dbterror.ClassExecutor.NewStd(errno.ErrPasswordExpireAnonymousUser)
	ErrMustChangePassword             = dbterror.ClassExecutor.NewStd(errno.ErrMustChangePassword)

	ErrWrongStringLength            = dbterror.ClassDDL.NewStd(errno.ErrWrongStringLength)
	ErrUnsupportedFlashbackTmpTable = dbterror.ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("Recover/flashback table is not supported on temporary tables", nil))
	ErrTruncateWrongInsertValue     = dbterror.ClassTable.NewStdErr(errno.ErrTruncatedWrongValue, errno.Message("Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d", nil))
	ErrExistsInHistoryPassword      = dbterror.ClassExecutor.NewStd(errno.ErrExistsInHistoryPassword)

	ErrWarnTooFewRecords              = dbterror.ClassExecutor.NewStd(errno.ErrWarnTooFewRecords)
	ErrWarnTooManyRecords             = dbterror.ClassExecutor.NewStd(errno.ErrWarnTooManyRecords)
	ErrLoadDataFromServerDisk         = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataFromServerDisk)
	ErrLoadParquetFromLocal           = dbterror.ClassExecutor.NewStd(errno.ErrLoadParquetFromLocal)
	ErrLoadDataEmptyPath              = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataEmptyPath)
	ErrLoadDataUnsupportedFormat      = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataUnsupportedFormat)
	ErrLoadDataInvalidURI             = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataInvalidURI)
	ErrLoadDataCantAccess             = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataCantAccess)
	ErrLoadDataCantRead               = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataCantRead)
	ErrLoadDataWrongFormatConfig      = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataWrongFormatConfig)
	ErrUnknownOption                  = dbterror.ClassExecutor.NewStd(errno.ErrUnknownOption)
	ErrInvalidOptionVal               = dbterror.ClassExecutor.NewStd(errno.ErrInvalidOptionVal)
	ErrDuplicateOption                = dbterror.ClassExecutor.NewStd(errno.ErrDuplicateOption)
	ErrLoadDataUnsupportedOption      = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataUnsupportedOption)
	ErrLoadDataJobNotFound            = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataJobNotFound)
	ErrLoadDataInvalidOperation       = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataInvalidOperation)
	ErrLoadDataLocalUnsupportedOption = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataLocalUnsupportedOption)
	ErrLoadDataPreCheckFailed         = dbterror.ClassExecutor.NewStd(errno.ErrLoadDataPreCheckFailed)
)
