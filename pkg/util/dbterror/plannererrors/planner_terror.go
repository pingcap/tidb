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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// error definitions.
var (
	ErrUnsupportedType                       = dbterror.ClassOptimizer.NewStd(errno.ErrUnsupportedType)
	ErrAnalyzeMissIndex                      = dbterror.ClassOptimizer.NewStd(errno.ErrAnalyzeMissIndex)
	ErrAnalyzeMissColumn                     = dbterror.ClassOptimizer.NewStd(errno.ErrAnalyzeMissColumn)
	ErrWrongParamCount                       = dbterror.ClassOptimizer.NewStd(errno.ErrWrongParamCount)
	ErrSchemaChanged                         = dbterror.ClassOptimizer.NewStd(errno.ErrSchemaChanged)
	ErrTablenameNotAllowedHere               = dbterror.ClassOptimizer.NewStd(errno.ErrTablenameNotAllowedHere)
	ErrNotSupportedYet                       = dbterror.ClassOptimizer.NewStd(errno.ErrNotSupportedYet)
	ErrWrongUsage                            = dbterror.ClassOptimizer.NewStd(errno.ErrWrongUsage)
	ErrUnknown                               = dbterror.ClassOptimizer.NewStd(errno.ErrUnknown)
	ErrUnknownTable                          = dbterror.ClassOptimizer.NewStd(errno.ErrUnknownTable)
	ErrNoSuchTable                           = dbterror.ClassOptimizer.NewStd(errno.ErrNoSuchTable)
	ErrViewRecursive                         = dbterror.ClassOptimizer.NewStd(errno.ErrViewRecursive)
	ErrWrongArguments                        = dbterror.ClassOptimizer.NewStd(errno.ErrWrongArguments)
	ErrWrongNumberOfColumnsInSelect          = dbterror.ClassOptimizer.NewStd(errno.ErrWrongNumberOfColumnsInSelect)
	ErrBadGeneratedColumn                    = dbterror.ClassOptimizer.NewStd(errno.ErrBadGeneratedColumn)
	ErrFieldNotInGroupBy                     = dbterror.ClassOptimizer.NewStd(errno.ErrFieldNotInGroupBy)
	ErrAggregateOrderNonAggQuery             = dbterror.ClassOptimizer.NewStd(errno.ErrAggregateOrderNonAggQuery)
	ErrFieldInOrderNotSelect                 = dbterror.ClassOptimizer.NewStd(errno.ErrFieldInOrderNotSelect)
	ErrAggregateInOrderNotSelect             = dbterror.ClassOptimizer.NewStd(errno.ErrAggregateInOrderNotSelect)
	ErrBadTable                              = dbterror.ClassOptimizer.NewStd(errno.ErrBadTable)
	ErrKeyDoesNotExist                       = dbterror.ClassOptimizer.NewStd(errno.ErrKeyDoesNotExist)
	ErrOperandColumns                        = dbterror.ClassOptimizer.NewStd(errno.ErrOperandColumns)
	ErrInvalidGroupFuncUse                   = dbterror.ClassOptimizer.NewStd(errno.ErrInvalidGroupFuncUse)
	ErrIllegalReference                      = dbterror.ClassOptimizer.NewStd(errno.ErrIllegalReference)
	ErrNoDB                                  = dbterror.ClassOptimizer.NewStd(errno.ErrNoDB)
	ErrUnknownExplainFormat                  = dbterror.ClassOptimizer.NewStd(errno.ErrUnknownExplainFormat)
	ErrWrongGroupField                       = dbterror.ClassOptimizer.NewStd(errno.ErrWrongGroupField)
	ErrDupFieldName                          = dbterror.ClassOptimizer.NewStd(errno.ErrDupFieldName)
	ErrNonUpdatableTable                     = dbterror.ClassOptimizer.NewStd(errno.ErrNonUpdatableTable)
	ErrMultiUpdateKeyConflict                = dbterror.ClassOptimizer.NewStd(errno.ErrMultiUpdateKeyConflict)
	ErrInternal                              = dbterror.ClassOptimizer.NewStd(errno.ErrInternal)
	ErrNonUniqTable                          = dbterror.ClassOptimizer.NewStd(errno.ErrNonuniqTable)
	ErrWindowInvalidWindowFuncUse            = dbterror.ClassOptimizer.NewStd(errno.ErrWindowInvalidWindowFuncUse)
	ErrWindowInvalidWindowFuncAliasUse       = dbterror.ClassOptimizer.NewStd(errno.ErrWindowInvalidWindowFuncAliasUse)
	ErrWindowNoSuchWindow                    = dbterror.ClassOptimizer.NewStd(errno.ErrWindowNoSuchWindow)
	ErrWindowCircularityInWindowGraph        = dbterror.ClassOptimizer.NewStd(errno.ErrWindowCircularityInWindowGraph)
	ErrWindowNoChildPartitioning             = dbterror.ClassOptimizer.NewStd(errno.ErrWindowNoChildPartitioning)
	ErrWindowNoInherentFrame                 = dbterror.ClassOptimizer.NewStd(errno.ErrWindowNoInherentFrame)
	ErrWindowNoRedefineOrderBy               = dbterror.ClassOptimizer.NewStd(errno.ErrWindowNoRedefineOrderBy)
	ErrWindowDuplicateName                   = dbterror.ClassOptimizer.NewStd(errno.ErrWindowDuplicateName)
	ErrPartitionClauseOnNonpartitioned       = dbterror.ClassOptimizer.NewStd(errno.ErrPartitionClauseOnNonpartitioned)
	ErrWindowFrameStartIllegal               = dbterror.ClassOptimizer.NewStd(errno.ErrWindowFrameStartIllegal)
	ErrWindowFrameEndIllegal                 = dbterror.ClassOptimizer.NewStd(errno.ErrWindowFrameEndIllegal)
	ErrWindowFrameIllegal                    = dbterror.ClassOptimizer.NewStd(errno.ErrWindowFrameIllegal)
	ErrWindowRangeFrameOrderType             = dbterror.ClassOptimizer.NewStd(errno.ErrWindowRangeFrameOrderType)
	ErrWindowRangeFrameTemporalType          = dbterror.ClassOptimizer.NewStd(errno.ErrWindowRangeFrameTemporalType)
	ErrWindowRangeFrameNumericType           = dbterror.ClassOptimizer.NewStd(errno.ErrWindowRangeFrameNumericType)
	ErrWindowRangeBoundNotConstant           = dbterror.ClassOptimizer.NewStd(errno.ErrWindowRangeBoundNotConstant)
	ErrWindowRowsIntervalUse                 = dbterror.ClassOptimizer.NewStd(errno.ErrWindowRowsIntervalUse)
	ErrWindowFunctionIgnoresFrame            = dbterror.ClassOptimizer.NewStd(errno.ErrWindowFunctionIgnoresFrame)
	ErrInvalidNumberOfArgs                   = dbterror.ClassOptimizer.NewStd(errno.ErrInvalidNumberOfArgs)
	ErrFieldInGroupingNotGroupBy             = dbterror.ClassOptimizer.NewStd(errno.ErrFieldInGroupingNotGroupBy)
	ErrUnsupportedOnGeneratedColumn          = dbterror.ClassOptimizer.NewStd(errno.ErrUnsupportedOnGeneratedColumn)
	ErrPrivilegeCheckFail                    = dbterror.ClassOptimizer.NewStd(errno.ErrPrivilegeCheckFail)
	ErrInvalidWildCard                       = dbterror.ClassOptimizer.NewStd(errno.ErrInvalidWildCard)
	ErrMixOfGroupFuncAndFields               = dbterror.ClassOptimizer.NewStd(errno.ErrMixOfGroupFuncAndFieldsIncompatible)
	ErrTooBigPrecision                       = dbterror.ClassExpression.NewStd(errno.ErrTooBigPrecision)
	ErrDBaccessDenied                        = dbterror.ClassOptimizer.NewStd(errno.ErrDBaccessDenied)
	ErrTableaccessDenied                     = dbterror.ClassOptimizer.NewStd(errno.ErrTableaccessDenied)
	ErrSpecificAccessDenied                  = dbterror.ClassOptimizer.NewStd(errno.ErrSpecificAccessDenied)
	ErrViewNoExplain                         = dbterror.ClassOptimizer.NewStd(errno.ErrViewNoExplain)
	ErrWrongValueCountOnRow                  = dbterror.ClassOptimizer.NewStd(errno.ErrWrongValueCountOnRow)
	ErrViewInvalid                           = dbterror.ClassOptimizer.NewStd(errno.ErrViewInvalid)
	ErrNoSuchThread                          = dbterror.ClassOptimizer.NewStd(errno.ErrNoSuchThread)
	ErrUnknownColumn                         = dbterror.ClassOptimizer.NewStd(errno.ErrBadField)
	ErrCartesianProductUnsupported           = dbterror.ClassOptimizer.NewStd(errno.ErrCartesianProductUnsupported)
	ErrStmtNotFound                          = dbterror.ClassOptimizer.NewStd(errno.ErrPreparedStmtNotFound)
	ErrAmbiguous                             = dbterror.ClassOptimizer.NewStd(errno.ErrNonUniq)
	ErrUnresolvedHintName                    = dbterror.ClassOptimizer.NewStd(errno.ErrUnresolvedHintName)
	ErrNotHintUpdatable                      = dbterror.ClassOptimizer.NewStd(errno.ErrNotHintUpdatable)
	ErrWarnConflictingHint                   = dbterror.ClassOptimizer.NewStd(errno.ErrWarnConflictingHint)
	ErrCTERecursiveRequiresUnion             = dbterror.ClassOptimizer.NewStd(errno.ErrCTERecursiveRequiresUnion)
	ErrCTERecursiveRequiresNonRecursiveFirst = dbterror.ClassOptimizer.NewStd(errno.ErrCTERecursiveRequiresNonRecursiveFirst)
	ErrCTERecursiveForbidsAggregation        = dbterror.ClassOptimizer.NewStd(errno.ErrCTERecursiveForbidsAggregation)
	ErrCTERecursiveForbiddenJoinOrder        = dbterror.ClassOptimizer.NewStd(errno.ErrCTERecursiveForbiddenJoinOrder)
	ErrInvalidRequiresSingleReference        = dbterror.ClassOptimizer.NewStd(errno.ErrInvalidRequiresSingleReference)
	ErrSQLInReadOnlyMode                     = dbterror.ClassOptimizer.NewStd(errno.ErrReadOnlyMode)
	ErrDeleteNotFoundColumn                  = dbterror.ClassOptimizer.NewStd(errno.ErrDeleteNotFoundColumn)
	// Since we cannot know if user logged in with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied              = dbterror.ClassOptimizer.NewStdErr(errno.ErrAccessDenied, errno.MySQLErrName[errno.ErrAccessDeniedNoPassword])
	ErrBadNull                   = dbterror.ClassOptimizer.NewStd(errno.ErrBadNull)
	ErrNotSupportedWithSem       = dbterror.ClassOptimizer.NewStd(errno.ErrNotSupportedWithSem)
	ErrAsOf                      = dbterror.ClassOptimizer.NewStd(errno.ErrAsOf)
	ErrOptOnTemporaryTable       = dbterror.ClassOptimizer.NewStd(errno.ErrOptOnTemporaryTable)
	ErrOptOnCacheTable           = dbterror.ClassOptimizer.NewStd(errno.ErrOptOnCacheTable)
	ErrDropTableOnTemporaryTable = dbterror.ClassOptimizer.NewStd(errno.ErrDropTableOnTemporaryTable)
	// ErrPartitionNoTemporary returns when partition at temporary mode
	ErrPartitionNoTemporary     = dbterror.ClassOptimizer.NewStd(errno.ErrPartitionNoTemporary)
	ErrViewSelectTemporaryTable = dbterror.ClassOptimizer.NewStd(errno.ErrViewSelectTmptable)
	ErrSubqueryMoreThan1Row     = dbterror.ClassOptimizer.NewStd(errno.ErrSubqueryNo1Row)
	ErrKeyPart0                 = dbterror.ClassOptimizer.NewStd(errno.ErrKeyPart0)
	ErrGettingNoopVariable      = dbterror.ClassOptimizer.NewStd(errno.ErrGettingNoopVariable)

	ErrPrepareMulti     = dbterror.ClassExecutor.NewStd(errno.ErrPrepareMulti)
	ErrUnsupportedPs    = dbterror.ClassExecutor.NewStd(errno.ErrUnsupportedPs)
	ErrPsManyParam      = dbterror.ClassExecutor.NewStd(errno.ErrPsManyParam)
	ErrPrepareDDL       = dbterror.ClassExecutor.NewStd(errno.ErrPrepareDDL)
	ErrRowIsReferenced2 = dbterror.ClassOptimizer.NewStd(errno.ErrRowIsReferenced2)
	ErrNoReferencedRow2 = dbterror.ClassOptimizer.NewStd(errno.ErrNoReferencedRow2)
)
