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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

// error definitions.
var (
	ErrUnsupportedType                 = terror.ClassOptimizer.NewStd(mysql.ErrUnsupportedType)
	ErrAnalyzeMissIndex                = terror.ClassOptimizer.NewStd(mysql.ErrAnalyzeMissIndex)
	ErrWrongParamCount                 = terror.ClassOptimizer.NewStd(mysql.ErrWrongParamCount)
	ErrSchemaChanged                   = terror.ClassOptimizer.NewStd(mysql.ErrSchemaChanged)
	ErrTablenameNotAllowedHere         = terror.ClassOptimizer.NewStd(mysql.ErrTablenameNotAllowedHere)
	ErrNotSupportedYet                 = terror.ClassOptimizer.NewStd(mysql.ErrNotSupportedYet)
	ErrWrongUsage                      = terror.ClassOptimizer.NewStd(mysql.ErrWrongUsage)
	ErrUnknown                         = terror.ClassOptimizer.NewStd(mysql.ErrUnknown)
	ErrUnknownTable                    = terror.ClassOptimizer.NewStd(mysql.ErrUnknownTable)
	ErrNoSuchTable                     = terror.ClassOptimizer.NewStd(mysql.ErrNoSuchTable)
	ErrWrongArguments                  = terror.ClassOptimizer.NewStd(mysql.ErrWrongArguments)
	ErrWrongNumberOfColumnsInSelect    = terror.ClassOptimizer.NewStd(mysql.ErrWrongNumberOfColumnsInSelect)
	ErrBadGeneratedColumn              = terror.ClassOptimizer.NewStd(mysql.ErrBadGeneratedColumn)
	ErrFieldNotInGroupBy               = terror.ClassOptimizer.NewStd(mysql.ErrFieldNotInGroupBy)
	ErrBadTable                        = terror.ClassOptimizer.NewStd(mysql.ErrBadTable)
	ErrKeyDoesNotExist                 = terror.ClassOptimizer.NewStd(mysql.ErrKeyDoesNotExist)
	ErrOperandColumns                  = terror.ClassOptimizer.NewStd(mysql.ErrOperandColumns)
	ErrInvalidGroupFuncUse             = terror.ClassOptimizer.NewStd(mysql.ErrInvalidGroupFuncUse)
	ErrIllegalReference                = terror.ClassOptimizer.NewStd(mysql.ErrIllegalReference)
	ErrNoDB                            = terror.ClassOptimizer.NewStd(mysql.ErrNoDB)
	ErrUnknownExplainFormat            = terror.ClassOptimizer.NewStd(mysql.ErrUnknownExplainFormat)
	ErrWrongGroupField                 = terror.ClassOptimizer.NewStd(mysql.ErrWrongGroupField)
	ErrDupFieldName                    = terror.ClassOptimizer.NewStd(mysql.ErrDupFieldName)
	ErrNonUpdatableTable               = terror.ClassOptimizer.NewStd(mysql.ErrNonUpdatableTable)
	ErrInternal                        = terror.ClassOptimizer.NewStd(mysql.ErrInternal)
	ErrNonUniqTable                    = terror.ClassOptimizer.NewStd(mysql.ErrNonuniqTable)
	ErrWindowInvalidWindowFuncUse      = terror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncUse)
	ErrWindowInvalidWindowFuncAliasUse = terror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncAliasUse)
	ErrWindowNoSuchWindow              = terror.ClassOptimizer.NewStd(mysql.ErrWindowNoSuchWindow)
	ErrWindowCircularityInWindowGraph  = terror.ClassOptimizer.NewStd(mysql.ErrWindowCircularityInWindowGraph)
	ErrWindowNoChildPartitioning       = terror.ClassOptimizer.NewStd(mysql.ErrWindowNoChildPartitioning)
	ErrWindowNoInherentFrame           = terror.ClassOptimizer.NewStd(mysql.ErrWindowNoInherentFrame)
	ErrWindowNoRedefineOrderBy         = terror.ClassOptimizer.NewStd(mysql.ErrWindowNoRedefineOrderBy)
	ErrWindowDuplicateName             = terror.ClassOptimizer.NewStd(mysql.ErrWindowDuplicateName)
	ErrPartitionClauseOnNonpartitioned = terror.ClassOptimizer.NewStd(mysql.ErrPartitionClauseOnNonpartitioned)
	ErrWindowFrameStartIllegal         = terror.ClassOptimizer.NewStd(mysql.ErrWindowFrameStartIllegal)
	ErrWindowFrameEndIllegal           = terror.ClassOptimizer.NewStd(mysql.ErrWindowFrameEndIllegal)
	ErrWindowFrameIllegal              = terror.ClassOptimizer.NewStd(mysql.ErrWindowFrameIllegal)
	ErrWindowRangeFrameOrderType       = terror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameOrderType)
	ErrWindowRangeFrameTemporalType    = terror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameTemporalType)
	ErrWindowRangeFrameNumericType     = terror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameNumericType)
	ErrWindowRangeBoundNotConstant     = terror.ClassOptimizer.NewStd(mysql.ErrWindowRangeBoundNotConstant)
	ErrWindowRowsIntervalUse           = terror.ClassOptimizer.NewStd(mysql.ErrWindowRowsIntervalUse)
	ErrWindowFunctionIgnoresFrame      = terror.ClassOptimizer.NewStd(mysql.ErrWindowFunctionIgnoresFrame)
	ErrUnsupportedOnGeneratedColumn    = terror.ClassOptimizer.NewStd(mysql.ErrUnsupportedOnGeneratedColumn)
	ErrPrivilegeCheckFail              = terror.ClassOptimizer.NewStd(mysql.ErrPrivilegeCheckFail)
	ErrInvalidWildCard                 = terror.ClassOptimizer.NewStd(mysql.ErrInvalidWildCard)
	ErrMixOfGroupFuncAndFields         = terror.ClassOptimizer.NewStd(mysql.ErrMixOfGroupFuncAndFieldsIncompatible)
	errTooBigPrecision                 = terror.ClassExpression.NewStd(mysql.ErrTooBigPrecision)
	ErrDBaccessDenied                  = terror.ClassOptimizer.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied               = terror.ClassOptimizer.NewStd(mysql.ErrTableaccessDenied)
	ErrSpecificAccessDenied            = terror.ClassOptimizer.NewStd(mysql.ErrSpecificAccessDenied)
	ErrViewNoExplain                   = terror.ClassOptimizer.NewStd(mysql.ErrViewNoExplain)
	ErrWrongValueCountOnRow            = terror.ClassOptimizer.NewStd(mysql.ErrWrongValueCountOnRow)
	ErrViewInvalid                     = terror.ClassOptimizer.NewStd(mysql.ErrViewInvalid)
	ErrNoSuchThread                    = terror.ClassOptimizer.NewStd(mysql.ErrNoSuchThread)
	ErrUnknownColumn                   = terror.ClassOptimizer.NewStd(mysql.ErrBadField)
	ErrCartesianProductUnsupported     = terror.ClassOptimizer.NewStd(mysql.ErrCartesianProductUnsupported)
	ErrStmtNotFound                    = terror.ClassOptimizer.NewStd(mysql.ErrPreparedStmtNotFound)
	ErrAmbiguous                       = terror.ClassOptimizer.NewStd(mysql.ErrNonUniq)
	// Since we cannot know if user logged in with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = terror.ClassOptimizer.NewStdErr(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword], "", "")
)
