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
	mysql "github.com/pingcap/tidb/v4/errno"
)

// error definitions.
var (
	ErrUnsupportedType                 = terror.ClassOptimizer.New(mysql.ErrUnsupportedType, mysql.MySQLErrName[mysql.ErrUnsupportedType])
	ErrAnalyzeMissIndex                = terror.ClassOptimizer.New(mysql.ErrAnalyzeMissIndex, mysql.MySQLErrName[mysql.ErrAnalyzeMissIndex])
	ErrWrongParamCount                 = terror.ClassOptimizer.New(mysql.ErrWrongParamCount, mysql.MySQLErrName[mysql.ErrWrongParamCount])
	ErrSchemaChanged                   = terror.ClassOptimizer.New(mysql.ErrSchemaChanged, mysql.MySQLErrName[mysql.ErrSchemaChanged])
	ErrTablenameNotAllowedHere         = terror.ClassOptimizer.New(mysql.ErrTablenameNotAllowedHere, mysql.MySQLErrName[mysql.ErrTablenameNotAllowedHere])
	ErrNotSupportedYet                 = terror.ClassOptimizer.New(mysql.ErrNotSupportedYet, mysql.MySQLErrName[mysql.ErrNotSupportedYet])
	ErrWrongUsage                      = terror.ClassOptimizer.New(mysql.ErrWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	ErrUnknown                         = terror.ClassOptimizer.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
	ErrUnknownTable                    = terror.ClassOptimizer.New(mysql.ErrUnknownTable, mysql.MySQLErrName[mysql.ErrUnknownTable])
	ErrNoSuchTable                     = terror.ClassOptimizer.New(mysql.ErrNoSuchTable, mysql.MySQLErrName[mysql.ErrNoSuchTable])
	ErrWrongArguments                  = terror.ClassOptimizer.New(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	ErrWrongNumberOfColumnsInSelect    = terror.ClassOptimizer.New(mysql.ErrWrongNumberOfColumnsInSelect, mysql.MySQLErrName[mysql.ErrWrongNumberOfColumnsInSelect])
	ErrBadGeneratedColumn              = terror.ClassOptimizer.New(mysql.ErrBadGeneratedColumn, mysql.MySQLErrName[mysql.ErrBadGeneratedColumn])
	ErrFieldNotInGroupBy               = terror.ClassOptimizer.New(mysql.ErrFieldNotInGroupBy, mysql.MySQLErrName[mysql.ErrFieldNotInGroupBy])
	ErrBadTable                        = terror.ClassOptimizer.New(mysql.ErrBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	ErrKeyDoesNotExist                 = terror.ClassOptimizer.New(mysql.ErrKeyDoesNotExist, mysql.MySQLErrName[mysql.ErrKeyDoesNotExist])
	ErrOperandColumns                  = terror.ClassOptimizer.New(mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrInvalidGroupFuncUse             = terror.ClassOptimizer.New(mysql.ErrInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	ErrIllegalReference                = terror.ClassOptimizer.New(mysql.ErrIllegalReference, mysql.MySQLErrName[mysql.ErrIllegalReference])
	ErrNoDB                            = terror.ClassOptimizer.New(mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
	ErrUnknownExplainFormat            = terror.ClassOptimizer.New(mysql.ErrUnknownExplainFormat, mysql.MySQLErrName[mysql.ErrUnknownExplainFormat])
	ErrWrongGroupField                 = terror.ClassOptimizer.New(mysql.ErrWrongGroupField, mysql.MySQLErrName[mysql.ErrWrongGroupField])
	ErrDupFieldName                    = terror.ClassOptimizer.New(mysql.ErrDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	ErrNonUpdatableTable               = terror.ClassOptimizer.New(mysql.ErrNonUpdatableTable, mysql.MySQLErrName[mysql.ErrNonUpdatableTable])
	ErrInternal                        = terror.ClassOptimizer.New(mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal])
	ErrNonUniqTable                    = terror.ClassOptimizer.New(mysql.ErrNonuniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	ErrWindowInvalidWindowFuncUse      = terror.ClassOptimizer.New(mysql.ErrWindowInvalidWindowFuncUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncUse])
	ErrWindowInvalidWindowFuncAliasUse = terror.ClassOptimizer.New(mysql.ErrWindowInvalidWindowFuncAliasUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncAliasUse])
	ErrWindowNoSuchWindow              = terror.ClassOptimizer.New(mysql.ErrWindowNoSuchWindow, mysql.MySQLErrName[mysql.ErrWindowNoSuchWindow])
	ErrWindowCircularityInWindowGraph  = terror.ClassOptimizer.New(mysql.ErrWindowCircularityInWindowGraph, mysql.MySQLErrName[mysql.ErrWindowCircularityInWindowGraph])
	ErrWindowNoChildPartitioning       = terror.ClassOptimizer.New(mysql.ErrWindowNoChildPartitioning, mysql.MySQLErrName[mysql.ErrWindowNoChildPartitioning])
	ErrWindowNoInherentFrame           = terror.ClassOptimizer.New(mysql.ErrWindowNoInherentFrame, mysql.MySQLErrName[mysql.ErrWindowNoInherentFrame])
	ErrWindowNoRedefineOrderBy         = terror.ClassOptimizer.New(mysql.ErrWindowNoRedefineOrderBy, mysql.MySQLErrName[mysql.ErrWindowNoRedefineOrderBy])
	ErrWindowDuplicateName             = terror.ClassOptimizer.New(mysql.ErrWindowDuplicateName, mysql.MySQLErrName[mysql.ErrWindowDuplicateName])
	ErrPartitionClauseOnNonpartitioned = terror.ClassOptimizer.New(mysql.ErrPartitionClauseOnNonpartitioned, mysql.MySQLErrName[mysql.ErrPartitionClauseOnNonpartitioned])
	ErrWindowFrameStartIllegal         = terror.ClassOptimizer.New(mysql.ErrWindowFrameStartIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameStartIllegal])
	ErrWindowFrameEndIllegal           = terror.ClassOptimizer.New(mysql.ErrWindowFrameEndIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameEndIllegal])
	ErrWindowFrameIllegal              = terror.ClassOptimizer.New(mysql.ErrWindowFrameIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameIllegal])
	ErrWindowRangeFrameOrderType       = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameOrderType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameOrderType])
	ErrWindowRangeFrameTemporalType    = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameTemporalType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameTemporalType])
	ErrWindowRangeFrameNumericType     = terror.ClassOptimizer.New(mysql.ErrWindowRangeFrameNumericType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameNumericType])
	ErrWindowRangeBoundNotConstant     = terror.ClassOptimizer.New(mysql.ErrWindowRangeBoundNotConstant, mysql.MySQLErrName[mysql.ErrWindowRangeBoundNotConstant])
	ErrWindowRowsIntervalUse           = terror.ClassOptimizer.New(mysql.ErrWindowRowsIntervalUse, mysql.MySQLErrName[mysql.ErrWindowRowsIntervalUse])
	ErrWindowFunctionIgnoresFrame      = terror.ClassOptimizer.New(mysql.ErrWindowFunctionIgnoresFrame, mysql.MySQLErrName[mysql.ErrWindowFunctionIgnoresFrame])
	ErrUnsupportedOnGeneratedColumn    = terror.ClassOptimizer.New(mysql.ErrUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	ErrPrivilegeCheckFail              = terror.ClassOptimizer.New(mysql.ErrPrivilegeCheckFail, mysql.MySQLErrName[mysql.ErrPrivilegeCheckFail])
	ErrInvalidWildCard                 = terror.ClassOptimizer.New(mysql.ErrInvalidWildCard, mysql.MySQLErrName[mysql.ErrInvalidWildCard])
	ErrMixOfGroupFuncAndFields         = terror.ClassOptimizer.New(mysql.ErrMixOfGroupFuncAndFieldsIncompatible, mysql.MySQLErrName[mysql.ErrMixOfGroupFuncAndFieldsIncompatible])
	errTooBigPrecision                 = terror.ClassExpression.New(mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	ErrDBaccessDenied                  = terror.ClassOptimizer.New(mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied               = terror.ClassOptimizer.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrSpecificAccessDenied            = terror.ClassOptimizer.New(mysql.ErrSpecificAccessDenied, mysql.MySQLErrName[mysql.ErrSpecificAccessDenied])
	ErrViewNoExplain                   = terror.ClassOptimizer.New(mysql.ErrViewNoExplain, mysql.MySQLErrName[mysql.ErrViewNoExplain])
	ErrWrongValueCountOnRow            = terror.ClassOptimizer.New(mysql.ErrWrongValueCountOnRow, mysql.MySQLErrName[mysql.ErrWrongValueCountOnRow])
	ErrViewInvalid                     = terror.ClassOptimizer.New(mysql.ErrViewInvalid, mysql.MySQLErrName[mysql.ErrViewInvalid])
	ErrNoSuchThread                    = terror.ClassOptimizer.New(mysql.ErrNoSuchThread, mysql.MySQLErrName[mysql.ErrNoSuchThread])
	ErrUnknownColumn                   = terror.ClassOptimizer.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	ErrCartesianProductUnsupported     = terror.ClassOptimizer.New(mysql.ErrCartesianProductUnsupported, mysql.MySQLErrName[mysql.ErrCartesianProductUnsupported])
	ErrStmtNotFound                    = terror.ClassOptimizer.New(mysql.ErrPreparedStmtNotFound, mysql.MySQLErrName[mysql.ErrPreparedStmtNotFound])
	ErrAmbiguous                       = terror.ClassOptimizer.New(mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
	// Since we cannot know if user loggined with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = terror.ClassOptimizer.New(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword])
)
