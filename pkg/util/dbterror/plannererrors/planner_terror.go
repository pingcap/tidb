// Copyright 2024 PingCAP, Inc.
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

package plannererrors

import (
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// error definitions.
var (
	ErrUnsupportedType                       = dbterror.ClassOptimizer.NewStd(mysql.ErrUnsupportedType)
	ErrAnalyzeMissIndex                      = dbterror.ClassOptimizer.NewStd(mysql.ErrAnalyzeMissIndex)
	ErrAnalyzeMissColumn                     = dbterror.ClassOptimizer.NewStd(mysql.ErrAnalyzeMissColumn)
	ErrWrongParamCount                       = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongParamCount)
	ErrSchemaChanged                         = dbterror.ClassOptimizer.NewStd(mysql.ErrSchemaChanged)
	ErrTablenameNotAllowedHere               = dbterror.ClassOptimizer.NewStd(mysql.ErrTablenameNotAllowedHere)
	ErrNotSupportedYet                       = dbterror.ClassOptimizer.NewStd(mysql.ErrNotSupportedYet)
	ErrWrongUsage                            = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongUsage)
	ErrUnknown                               = dbterror.ClassOptimizer.NewStd(mysql.ErrUnknown)
	ErrUnknownTable                          = dbterror.ClassOptimizer.NewStd(mysql.ErrUnknownTable)
	ErrNoSuchTable                           = dbterror.ClassOptimizer.NewStd(mysql.ErrNoSuchTable)
	ErrViewRecursive                         = dbterror.ClassOptimizer.NewStd(mysql.ErrViewRecursive)
	ErrWrongArguments                        = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongArguments)
	ErrWrongNumberOfColumnsInSelect          = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongNumberOfColumnsInSelect)
	ErrBadGeneratedColumn                    = dbterror.ClassOptimizer.NewStd(mysql.ErrBadGeneratedColumn)
	ErrFieldNotInGroupBy                     = dbterror.ClassOptimizer.NewStd(mysql.ErrFieldNotInGroupBy)
	ErrAggregateOrderNonAggQuery             = dbterror.ClassOptimizer.NewStd(mysql.ErrAggregateOrderNonAggQuery)
	ErrFieldInOrderNotSelect                 = dbterror.ClassOptimizer.NewStd(mysql.ErrFieldInOrderNotSelect)
	ErrAggregateInOrderNotSelect             = dbterror.ClassOptimizer.NewStd(mysql.ErrAggregateInOrderNotSelect)
	ErrBadTable                              = dbterror.ClassOptimizer.NewStd(mysql.ErrBadTable)
	ErrKeyDoesNotExist                       = dbterror.ClassOptimizer.NewStd(mysql.ErrKeyDoesNotExist)
	ErrOperandColumns                        = dbterror.ClassOptimizer.NewStd(mysql.ErrOperandColumns)
	ErrInvalidGroupFuncUse                   = dbterror.ClassOptimizer.NewStd(mysql.ErrInvalidGroupFuncUse)
	ErrIllegalReference                      = dbterror.ClassOptimizer.NewStd(mysql.ErrIllegalReference)
	ErrNoDB                                  = dbterror.ClassOptimizer.NewStd(mysql.ErrNoDB)
	ErrUnknownExplainFormat                  = dbterror.ClassOptimizer.NewStd(mysql.ErrUnknownExplainFormat)
	ErrWrongGroupField                       = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongGroupField)
	ErrDupFieldName                          = dbterror.ClassOptimizer.NewStd(mysql.ErrDupFieldName)
	ErrNonUpdatableTable                     = dbterror.ClassOptimizer.NewStd(mysql.ErrNonUpdatableTable)
	ErrMultiUpdateKeyConflict                = dbterror.ClassOptimizer.NewStd(mysql.ErrMultiUpdateKeyConflict)
	ErrInternal                              = dbterror.ClassOptimizer.NewStd(mysql.ErrInternal)
	ErrNonUniqTable                          = dbterror.ClassOptimizer.NewStd(mysql.ErrNonuniqTable)
	ErrWindowInvalidWindowFuncUse            = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncUse)
	ErrWindowInvalidWindowFuncAliasUse       = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowInvalidWindowFuncAliasUse)
	ErrWindowNoSuchWindow                    = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowNoSuchWindow)
	ErrWindowCircularityInWindowGraph        = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowCircularityInWindowGraph)
	ErrWindowNoChildPartitioning             = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowNoChildPartitioning)
	ErrWindowNoInherentFrame                 = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowNoInherentFrame)
	ErrWindowNoRedefineOrderBy               = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowNoRedefineOrderBy)
	ErrWindowDuplicateName                   = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowDuplicateName)
	ErrPartitionClauseOnNonpartitioned       = dbterror.ClassOptimizer.NewStd(mysql.ErrPartitionClauseOnNonpartitioned)
	ErrWindowFrameStartIllegal               = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowFrameStartIllegal)
	ErrWindowFrameEndIllegal                 = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowFrameEndIllegal)
	ErrWindowFrameIllegal                    = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowFrameIllegal)
	ErrWindowRangeFrameOrderType             = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameOrderType)
	ErrWindowRangeFrameTemporalType          = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameTemporalType)
	ErrWindowRangeFrameNumericType           = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowRangeFrameNumericType)
	ErrWindowRangeBoundNotConstant           = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowRangeBoundNotConstant)
	ErrWindowRowsIntervalUse                 = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowRowsIntervalUse)
	ErrWindowFunctionIgnoresFrame            = dbterror.ClassOptimizer.NewStd(mysql.ErrWindowFunctionIgnoresFrame)
	ErrInvalidNumberOfArgs                   = dbterror.ClassOptimizer.NewStd(mysql.ErrInvalidNumberOfArgs)
	ErrFieldInGroupingNotGroupBy             = dbterror.ClassOptimizer.NewStd(mysql.ErrFieldInGroupingNotGroupBy)
	ErrUnsupportedOnGeneratedColumn          = dbterror.ClassOptimizer.NewStd(mysql.ErrUnsupportedOnGeneratedColumn)
	ErrPrivilegeCheckFail                    = dbterror.ClassOptimizer.NewStd(mysql.ErrPrivilegeCheckFail)
	ErrInvalidWildCard                       = dbterror.ClassOptimizer.NewStd(mysql.ErrInvalidWildCard)
	ErrMixOfGroupFuncAndFields               = dbterror.ClassOptimizer.NewStd(mysql.ErrMixOfGroupFuncAndFieldsIncompatible)
	ErrTooBigPrecision                       = dbterror.ClassExpression.NewStd(mysql.ErrTooBigPrecision)
	ErrDBaccessDenied                        = dbterror.ClassOptimizer.NewStd(mysql.ErrDBaccessDenied)
	ErrTableaccessDenied                     = dbterror.ClassOptimizer.NewStd(mysql.ErrTableaccessDenied)
	ErrSpecificAccessDenied                  = dbterror.ClassOptimizer.NewStd(mysql.ErrSpecificAccessDenied)
	ErrViewNoExplain                         = dbterror.ClassOptimizer.NewStd(mysql.ErrViewNoExplain)
	ErrWrongValueCountOnRow                  = dbterror.ClassOptimizer.NewStd(mysql.ErrWrongValueCountOnRow)
	ErrViewInvalid                           = dbterror.ClassOptimizer.NewStd(mysql.ErrViewInvalid)
	ErrNoSuchThread                          = dbterror.ClassOptimizer.NewStd(mysql.ErrNoSuchThread)
	ErrUnknownColumn                         = dbterror.ClassOptimizer.NewStd(mysql.ErrBadField)
	ErrCartesianProductUnsupported           = dbterror.ClassOptimizer.NewStd(mysql.ErrCartesianProductUnsupported)
	ErrStmtNotFound                          = dbterror.ClassOptimizer.NewStd(mysql.ErrPreparedStmtNotFound)
	ErrAmbiguous                             = dbterror.ClassOptimizer.NewStd(mysql.ErrNonUniq)
	ErrUnresolvedHintName                    = dbterror.ClassOptimizer.NewStd(mysql.ErrUnresolvedHintName)
	ErrNotHintUpdatable                      = dbterror.ClassOptimizer.NewStd(mysql.ErrNotHintUpdatable)
	ErrWarnConflictingHint                   = dbterror.ClassOptimizer.NewStd(mysql.ErrWarnConflictingHint)
	ErrCTERecursiveRequiresUnion             = dbterror.ClassOptimizer.NewStd(mysql.ErrCTERecursiveRequiresUnion)
	ErrCTERecursiveRequiresNonRecursiveFirst = dbterror.ClassOptimizer.NewStd(mysql.ErrCTERecursiveRequiresNonRecursiveFirst)
	ErrCTERecursiveForbidsAggregation        = dbterror.ClassOptimizer.NewStd(mysql.ErrCTERecursiveForbidsAggregation)
	ErrCTERecursiveForbiddenJoinOrder        = dbterror.ClassOptimizer.NewStd(mysql.ErrCTERecursiveForbiddenJoinOrder)
	ErrInvalidRequiresSingleReference        = dbterror.ClassOptimizer.NewStd(mysql.ErrInvalidRequiresSingleReference)
	ErrSQLInReadOnlyMode                     = dbterror.ClassOptimizer.NewStd(mysql.ErrReadOnlyMode)
	// Since we cannot know if user logged in with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied              = dbterror.ClassOptimizer.NewStdErr(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword])
	ErrBadNull                   = dbterror.ClassOptimizer.NewStd(mysql.ErrBadNull)
	ErrNotSupportedWithSem       = dbterror.ClassOptimizer.NewStd(mysql.ErrNotSupportedWithSem)
	ErrAsOf                      = dbterror.ClassOptimizer.NewStd(mysql.ErrAsOf)
	ErrOptOnTemporaryTable       = dbterror.ClassOptimizer.NewStd(mysql.ErrOptOnTemporaryTable)
	ErrOptOnCacheTable           = dbterror.ClassOptimizer.NewStd(mysql.ErrOptOnCacheTable)
	ErrDropTableOnTemporaryTable = dbterror.ClassOptimizer.NewStd(mysql.ErrDropTableOnTemporaryTable)
	// ErrPartitionNoTemporary returns when partition at temporary mode
	ErrPartitionNoTemporary     = dbterror.ClassOptimizer.NewStd(mysql.ErrPartitionNoTemporary)
	ErrViewSelectTemporaryTable = dbterror.ClassOptimizer.NewStd(mysql.ErrViewSelectTmptable)
	ErrSubqueryMoreThan1Row     = dbterror.ClassOptimizer.NewStd(mysql.ErrSubqueryNo1Row)
	ErrKeyPart0                 = dbterror.ClassOptimizer.NewStd(mysql.ErrKeyPart0)
	ErrGettingNoopVariable      = dbterror.ClassOptimizer.NewStd(mysql.ErrGettingNoopVariable)

	ErrPrepareMulti     = dbterror.ClassExecutor.NewStd(mysql.ErrPrepareMulti)
	ErrUnsupportedPs    = dbterror.ClassExecutor.NewStd(mysql.ErrUnsupportedPs)
	ErrPsManyParam      = dbterror.ClassExecutor.NewStd(mysql.ErrPsManyParam)
	ErrPrepareDDL       = dbterror.ClassExecutor.NewStd(mysql.ErrPrepareDDL)
	ErrRowIsReferenced2 = dbterror.ClassOptimizer.NewStd(mysql.ErrRowIsReferenced2)
	ErrNoReferencedRow2 = dbterror.ClassOptimizer.NewStd(mysql.ErrNoReferencedRow2)
)
