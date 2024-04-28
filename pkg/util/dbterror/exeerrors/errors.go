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
	mysql "github.com/pingcap/tidb/pkg/errno"
	parser_mysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
	ErrIllegalGrantForTable = dbterror.ClassExecutor.NewStd(mysql.ErrIllegalGrantForTable)
	ErrColumnsNotMatched    = dbterror.ClassExecutor.NewStd(mysql.ErrColumnNotMatched)

	ErrCantCreateUserWithGrant              = dbterror.ClassExecutor.NewStd(mysql.ErrCantCreateUserWithGrant)
	ErrPasswordNoMatch                      = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordNoMatch)
	ErrCannotUser                           = dbterror.ClassExecutor.NewStd(mysql.ErrCannotUser)
	ErrGrantRole                            = dbterror.ClassExecutor.NewStd(mysql.ErrGrantRole)
	ErrPasswordFormat                       = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordFormat)
	ErrCantChangeTxCharacteristics          = dbterror.ClassExecutor.NewStd(mysql.ErrCantChangeTxCharacteristics)
	ErrPsManyParam                          = dbterror.ClassExecutor.NewStd(mysql.ErrPsManyParam)
	ErrAdminCheckTable                      = dbterror.ClassExecutor.NewStd(mysql.ErrAdminCheckTable)
	ErrDBaccessDenied                       = dbterror.ClassExecutor.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied                    = dbterror.ClassExecutor.NewStd(mysql.ErrTableaccessDenied)
	ErrBadDB                                = dbterror.ClassExecutor.NewStd(mysql.ErrBadDB)
	ErrWrongObject                          = dbterror.ClassExecutor.NewStd(mysql.ErrWrongObject)
	ErrWrongUsage                           = dbterror.ClassExecutor.NewStd(mysql.ErrWrongUsage)
	ErrRoleNotGranted                       = dbterror.ClassPrivilege.NewStd(mysql.ErrRoleNotGranted)
	ErrDeadlock                             = dbterror.ClassExecutor.NewStd(mysql.ErrLockDeadlock)
	ErrQueryInterrupted                     = dbterror.ClassExecutor.NewStd(mysql.ErrQueryInterrupted)
	ErrMaxExecTimeExceeded                  = dbterror.ClassExecutor.NewStd(mysql.ErrMaxExecTimeExceeded)
	ErrResourceGroupQueryRunawayInterrupted = dbterror.ClassExecutor.NewStd(mysql.ErrResourceGroupQueryRunawayInterrupted)
	ErrResourceGroupQueryRunawayQuarantine  = dbterror.ClassExecutor.NewStd(mysql.ErrResourceGroupQueryRunawayQuarantine)
	ErrDynamicPrivilegeNotRegistered        = dbterror.ClassExecutor.NewStd(mysql.ErrDynamicPrivilegeNotRegistered)
	ErrIllegalPrivilegeLevel                = dbterror.ClassExecutor.NewStd(mysql.ErrIllegalPrivilegeLevel)
	ErrInvalidSplitRegionRanges             = dbterror.ClassExecutor.NewStd(mysql.ErrInvalidSplitRegionRanges)
	ErrViewInvalid                          = dbterror.ClassExecutor.NewStd(mysql.ErrViewInvalid)
	ErrInstanceScope                        = dbterror.ClassExecutor.NewStd(mysql.ErrInstanceScope)
	ErrSettingNoopVariable                  = dbterror.ClassExecutor.NewStd(mysql.ErrSettingNoopVariable)
	ErrLazyUniquenessCheckFailure           = dbterror.ClassExecutor.NewStd(mysql.ErrLazyUniquenessCheckFailure)
	ErrMemoryExceedForQuery                 = dbterror.ClassExecutor.NewStd(mysql.ErrMemoryExceedForQuery)
	ErrMemoryExceedForInstance              = dbterror.ClassExecutor.NewStd(mysql.ErrMemoryExceedForInstance)

	ErrBRIEBackupFailed               = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEBackupFailed)
	ErrBRIERestoreFailed              = dbterror.ClassExecutor.NewStd(mysql.ErrBRIERestoreFailed)
	ErrBRIEImportFailed               = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEImportFailed)
	ErrBRIEExportFailed               = dbterror.ClassExecutor.NewStd(mysql.ErrBRIEExportFailed)
	ErrBRJobNotFound                  = dbterror.ClassExecutor.NewStd(mysql.ErrBRJobNotFound)
	ErrCTEMaxRecursionDepth           = dbterror.ClassExecutor.NewStd(mysql.ErrCTEMaxRecursionDepth)
	ErrPluginIsNotLoaded              = dbterror.ClassExecutor.NewStd(mysql.ErrPluginIsNotLoaded)
	ErrSetPasswordAuthPlugin          = dbterror.ClassExecutor.NewStd(mysql.ErrSetPasswordAuthPlugin)
	ErrFuncNotEnabled                 = dbterror.ClassExecutor.NewStdErr(mysql.ErrNotSupportedYet, parser_mysql.Message("%-.32s is not supported. To enable this experimental feature, set '%-.32s' in the configuration file.", nil))
	ErrSavepointNotExists             = dbterror.ClassExecutor.NewStd(mysql.ErrSpDoesNotExist)
	ErrForeignKeyCascadeDepthExceeded = dbterror.ClassExecutor.NewStd(mysql.ErrForeignKeyCascadeDepthExceeded)
	ErrPasswordExpireAnonymousUser    = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordExpireAnonymousUser)
	ErrMustChangePassword             = dbterror.ClassExecutor.NewStd(mysql.ErrMustChangePassword)

	ErrWrongStringLength            = dbterror.ClassDDL.NewStd(mysql.ErrWrongStringLength)
	ErrUnsupportedFlashbackTmpTable = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("Recover/flashback table is not supported on temporary tables", nil))
	ErrTruncateWrongInsertValue     = dbterror.ClassTable.NewStdErr(mysql.ErrTruncatedWrongValue, parser_mysql.Message("Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d", nil))
	ErrExistsInHistoryPassword      = dbterror.ClassExecutor.NewStd(mysql.ErrExistsInHistoryPassword)

	ErrWarnTooFewRecords              = dbterror.ClassExecutor.NewStd(mysql.ErrWarnTooFewRecords)
	ErrWarnTooManyRecords             = dbterror.ClassExecutor.NewStd(mysql.ErrWarnTooManyRecords)
	ErrLoadDataFromServerDisk         = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataFromServerDisk)
	ErrLoadParquetFromLocal           = dbterror.ClassExecutor.NewStd(mysql.ErrLoadParquetFromLocal)
	ErrLoadDataEmptyPath              = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataEmptyPath)
	ErrLoadDataUnsupportedFormat      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataUnsupportedFormat)
	ErrLoadDataInvalidURI             = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataInvalidURI)
	ErrLoadDataCantAccess             = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCantAccess)
	ErrLoadDataCantRead               = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataCantRead)
	ErrLoadDataWrongFormatConfig      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataWrongFormatConfig)
	ErrUnknownOption                  = dbterror.ClassExecutor.NewStd(mysql.ErrUnknownOption)
	ErrInvalidOptionVal               = dbterror.ClassExecutor.NewStd(mysql.ErrInvalidOptionVal)
	ErrDuplicateOption                = dbterror.ClassExecutor.NewStd(mysql.ErrDuplicateOption)
	ErrLoadDataUnsupportedOption      = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataUnsupportedOption)
	ErrLoadDataJobNotFound            = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataJobNotFound)
	ErrLoadDataInvalidOperation       = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataInvalidOperation)
	ErrLoadDataLocalUnsupportedOption = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataLocalUnsupportedOption)
	ErrLoadDataPreCheckFailed         = dbterror.ClassExecutor.NewStd(mysql.ErrLoadDataPreCheckFailed)
)
