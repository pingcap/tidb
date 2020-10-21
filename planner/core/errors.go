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

package core

import (
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

// error definitions.
var (
	ErrUnsupportedType                 = dberror.ClassOptimizer.NewStd(mysql.ErrUnsupportedType)
	ErrAnalyzeMissIndex                = dberror.ClassOptimizer.NewStd(mysql.ErrAnalyzeMissIndex)
	ErrWrongParamCount                 = dberror.ClassOptimizer.NewStd(mysql.ErrWrongParamCount)
	ErrSchemaChanged                   = dberror.ClassOptimizer.NewStd(mysql.ErrSchemaChanged)
	ErrTablenameNotAllowedHere         = dberror.ClassOptimizer.NewStd(mysql.ErrTablenameNotAllowedHere)
	ErrNotSupportedYet                 = dberror.ClassOptimizer.NewStd(mysql.ErrNotSupportedYet)
	ErrWrongUsage                      = dberror.ClassOptimizer.NewStd(mysql.ErrWrongUsage)
	ErrUnknown                         = dberror.ClassOptimizer.NewStd(mysql.ErrUnknown)
	ErrUnknownTable                    = dberror.ClassOptimizer.NewStd(mysql.ErrUnknownTable)
	ErrNoSuchTable                     = dberror.ClassOptimizer.NewStd(mysql.ErrNoSuchTable)
	ErrViewRecursive                   = dberror.ClassOptimizer.NewStd(mysql.ErrViewRecursive)
	ErrWrongArguments                  = dberror.ClassOptimizer.NewStd(mysql.ErrWrongArguments)
	ErrWrongNumberOfColumnsInSelect    = dberror.ClassOptimizer.NewStd(mysql.ErrWrongNumberOfColumnsInSelect)
	ErrBadGeneratedColumn              = dberror.ClassOptimizer.NewStd(mysql.ErrBadGeneratedColumn)
	ErrFieldNotInGroupBy               = dberror.ClassOptimizer.NewStd(mysql.ErrFieldNotInGroupBy)
	ErrBadTable                        = dberror.ClassOptimizer.NewStd(mysql.ErrBadTable)
	ErrKeyDoesNotExist                 = dberror.ClassOptimizer.NewStd(mysql.ErrKeyDoesNotExist)
	ErrOperandColumns                  = dberror.ClassOptimizer.NewStd(mysql.ErrOperandColumns)
	ErrInvalidGroupFuncUse             = dberror.ClassOptimizer.NewStd(mysql.ErrInvalidGroupFuncUse)
	ErrIllegalReference                = dberror.ClassOptimizer.NewStd(mysql.ErrIllegalReference)
	ErrNoDB                            = dberror.ClassOptimizer.NewStd(mysql.ErrNoDB)
	ErrUnknownExplainFormat            = dberror.ClassOptimizer.NewStd(mysql.ErrUnknownExplainFormat)
	ErrWrongGroupField                 = dberror.ClassOptimizer.NewStd(mysql.ErrWrongGroupField)
	ErrDupFieldName                    = dberror.ClassOptimizer.NewStd(mysql.ErrDupFieldName)
	ErrNonUpdatableTable               = dberror.ClassOptimizer.NewStd(mysql.ErrNonUpdatableTable)
	ErrInternal                        = dberror.ClassOptimizer.NewStd(mysql.ErrInternal)
	ErrNonUniqTable                    = dberror.ClassOptimizer.NewStd(mysql.ErrNonuniqTable)
	ErrWindowInvalidWindowFuncUse      = dberror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncUse)
	ErrWindowInvalidWindowFuncAliasUse = dberror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncAliasUse)
	ErrWindowNoSuchWindow              = dberror.ClassOptimizer.NewStd(mysql.ErrWindowNoSuchWindow)
	ErrWindowCircularityInWindowGraph  = dberror.ClassOptimizer.NewStd(mysql.ErrWindowCircularityInWindowGraph)
	ErrWindowNoChildPartitioning       = dberror.ClassOptimizer.NewStd(mysql.ErrWindowNoChildPartitioning)
	ErrWindowNoInherentFrame           = dberror.ClassOptimizer.NewStd(mysql.ErrWindowNoInherentFrame)
	ErrWindowNoRedefineOrderBy         = dberror.ClassOptimizer.NewStd(mysql.ErrWindowNoRedefineOrderBy)
	ErrWindowDuplicateName             = dberror.ClassOptimizer.NewStd(mysql.ErrWindowDuplicateName)
	ErrPartitionClauseOnNonpartitioned = dberror.ClassOptimizer.NewStd(mysql.ErrPartitionClauseOnNonpartitioned)
	ErrWindowFrameStartIllegal         = dberror.ClassOptimizer.NewStd(mysql.ErrWindowFrameStartIllegal)
	ErrWindowFrameEndIllegal           = dberror.ClassOptimizer.NewStd(mysql.ErrWindowFrameEndIllegal)
	ErrWindowFrameIllegal              = dberror.ClassOptimizer.NewStd(mysql.ErrWindowFrameIllegal)
	ErrWindowRangeFrameOrderType       = dberror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameOrderType)
	ErrWindowRangeFrameTemporalType    = dberror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameTemporalType)
	ErrWindowRangeFrameNumericType     = dberror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameNumericType)
	ErrWindowRangeBoundNotConstant     = dberror.ClassOptimizer.NewStd(mysql.ErrWindowRangeBoundNotConstant)
	ErrWindowRowsIntervalUse           = dberror.ClassOptimizer.NewStd(mysql.ErrWindowRowsIntervalUse)
	ErrWindowFunctionIgnoresFrame      = dberror.ClassOptimizer.NewStd(mysql.ErrWindowFunctionIgnoresFrame)
	ErrUnsupportedOnGeneratedColumn    = dberror.ClassOptimizer.NewStd(mysql.ErrUnsupportedOnGeneratedColumn)
	ErrPrivilegeCheckFail              = dberror.ClassOptimizer.NewStd(mysql.ErrPrivilegeCheckFail)
	ErrInvalidWildCard                 = dberror.ClassOptimizer.NewStd(mysql.ErrInvalidWildCard)
	ErrMixOfGroupFuncAndFields         = dberror.ClassOptimizer.NewStd(mysql.ErrMixOfGroupFuncAndFieldsIncompatible)
	errTooBigPrecision                 = terror.ClassExpression.New(mysql.ErrTooBigPrecision)
	ErrDBaccessDenied                  = dberror.ClassOptimizer.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied               = dberror.ClassOptimizer.NewStd(mysql.ErrTableaccessDenied)
	ErrSpecificAccessDenied            = dberror.ClassOptimizer.NewStd(mysql.ErrSpecificAccessDenied)
	ErrViewNoExplain                   = dberror.ClassOptimizer.NewStd(mysql.ErrViewNoExplain)
	ErrWrongValueCountOnRow            = dberror.ClassOptimizer.NewStd(mysql.ErrWrongValueCountOnRow)
	ErrViewInvalid                     = dberror.ClassOptimizer.NewStd(mysql.ErrViewInvalid)
	ErrNoSuchThread                    = dberror.ClassOptimizer.NewStd(mysql.ErrNoSuchThread)
	ErrUnknownColumn                   = dberror.ClassOptimizer.NewStd(mysql.ErrBadField)
	ErrCartesianProductUnsupported     = dberror.ClassOptimizer.NewStd(mysql.ErrCartesianProductUnsupported)
	ErrStmtNotFound                    = dberror.ClassOptimizer.NewStd(mysql.ErrPreparedStmtNotFound)
	ErrAmbiguous                       = dberror.ClassOptimizer.NewStd(mysql.ErrNonUniq)
	// Since we cannot know if user logged in with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = dbdberror.ClassOptimizer.NewStdStdErr(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword], "", "")
)
