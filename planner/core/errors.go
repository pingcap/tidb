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
	pterror "github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// error definitions.
var (
	ErrUnsupportedType                 = terror.New(pterror.ClassOptimizer, mysql.ErrUnsupportedType, mysql.MySQLErrName[mysql.ErrUnsupportedType])
	ErrAnalyzeMissIndex                = terror.New(pterror.ClassOptimizer, mysql.ErrAnalyzeMissIndex, mysql.MySQLErrName[mysql.ErrAnalyzeMissIndex])
	ErrWrongParamCount                 = terror.New(pterror.ClassOptimizer, mysql.ErrWrongParamCount, mysql.MySQLErrName[mysql.ErrWrongParamCount])
	ErrSchemaChanged                   = terror.New(pterror.ClassOptimizer, mysql.ErrSchemaChanged, mysql.MySQLErrName[mysql.ErrSchemaChanged])
	ErrTablenameNotAllowedHere         = terror.New(pterror.ClassOptimizer, mysql.ErrTablenameNotAllowedHere, mysql.MySQLErrName[mysql.ErrTablenameNotAllowedHere])
	ErrNotSupportedYet                 = terror.New(pterror.ClassOptimizer, mysql.ErrNotSupportedYet, mysql.MySQLErrName[mysql.ErrNotSupportedYet])
	ErrWrongUsage                      = terror.New(pterror.ClassOptimizer, mysql.ErrWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	ErrUnknown                         = terror.New(pterror.ClassOptimizer, mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
	ErrUnknownTable                    = terror.New(pterror.ClassOptimizer, mysql.ErrUnknownTable, mysql.MySQLErrName[mysql.ErrUnknownTable])
	ErrWrongArguments                  = terror.New(pterror.ClassOptimizer, mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	ErrWrongNumberOfColumnsInSelect    = terror.New(pterror.ClassOptimizer, mysql.ErrWrongNumberOfColumnsInSelect, mysql.MySQLErrName[mysql.ErrWrongNumberOfColumnsInSelect])
	ErrBadGeneratedColumn              = terror.New(pterror.ClassOptimizer, mysql.ErrBadGeneratedColumn, mysql.MySQLErrName[mysql.ErrBadGeneratedColumn])
	ErrFieldNotInGroupBy               = terror.New(pterror.ClassOptimizer, mysql.ErrFieldNotInGroupBy, mysql.MySQLErrName[mysql.ErrFieldNotInGroupBy])
	ErrBadTable                        = terror.New(pterror.ClassOptimizer, mysql.ErrBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	ErrKeyDoesNotExist                 = terror.New(pterror.ClassOptimizer, mysql.ErrKeyDoesNotExist, mysql.MySQLErrName[mysql.ErrKeyDoesNotExist])
	ErrOperandColumns                  = terror.New(pterror.ClassOptimizer, mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrInvalidGroupFuncUse             = terror.New(pterror.ClassOptimizer, mysql.ErrInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	ErrIllegalReference                = terror.New(pterror.ClassOptimizer, mysql.ErrIllegalReference, mysql.MySQLErrName[mysql.ErrIllegalReference])
	ErrNoDB                            = terror.New(pterror.ClassOptimizer, mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
	ErrUnknownExplainFormat            = terror.New(pterror.ClassOptimizer, mysql.ErrUnknownExplainFormat, mysql.MySQLErrName[mysql.ErrUnknownExplainFormat])
	ErrWrongGroupField                 = terror.New(pterror.ClassOptimizer, mysql.ErrWrongGroupField, mysql.MySQLErrName[mysql.ErrWrongGroupField])
	ErrDupFieldName                    = terror.New(pterror.ClassOptimizer, mysql.ErrDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	ErrNonUpdatableTable               = terror.New(pterror.ClassOptimizer, mysql.ErrNonUpdatableTable, mysql.MySQLErrName[mysql.ErrNonUpdatableTable])
	ErrInternal                        = terror.New(pterror.ClassOptimizer, mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal])
	ErrNonUniqTable                    = terror.New(pterror.ClassOptimizer, mysql.ErrNonuniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	ErrWindowInvalidWindowFuncUse      = terror.New(pterror.ClassOptimizer, mysql.ErrWindowInvalidWindowFuncUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncUse])
	ErrWindowInvalidWindowFuncAliasUse = terror.New(pterror.ClassOptimizer, mysql.ErrWindowInvalidWindowFuncAliasUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncAliasUse])
	ErrWindowNoSuchWindow              = terror.New(pterror.ClassOptimizer, mysql.ErrWindowNoSuchWindow, mysql.MySQLErrName[mysql.ErrWindowNoSuchWindow])
	ErrWindowCircularityInWindowGraph  = terror.New(pterror.ClassOptimizer, mysql.ErrWindowCircularityInWindowGraph, mysql.MySQLErrName[mysql.ErrWindowCircularityInWindowGraph])
	ErrWindowNoChildPartitioning       = terror.New(pterror.ClassOptimizer, mysql.ErrWindowNoChildPartitioning, mysql.MySQLErrName[mysql.ErrWindowNoChildPartitioning])
	ErrWindowNoInherentFrame           = terror.New(pterror.ClassOptimizer, mysql.ErrWindowNoInherentFrame, mysql.MySQLErrName[mysql.ErrWindowNoInherentFrame])
	ErrWindowNoRedefineOrderBy         = terror.New(pterror.ClassOptimizer, mysql.ErrWindowNoRedefineOrderBy, mysql.MySQLErrName[mysql.ErrWindowNoRedefineOrderBy])
	ErrWindowDuplicateName             = terror.New(pterror.ClassOptimizer, mysql.ErrWindowDuplicateName, mysql.MySQLErrName[mysql.ErrWindowDuplicateName])
	ErrPartitionClauseOnNonpartitioned = terror.New(pterror.ClassOptimizer, mysql.ErrPartitionClauseOnNonpartitioned, mysql.MySQLErrName[mysql.ErrPartitionClauseOnNonpartitioned])
	ErrWindowFrameStartIllegal         = terror.New(pterror.ClassOptimizer, mysql.ErrWindowFrameStartIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameStartIllegal])
	ErrWindowFrameEndIllegal           = terror.New(pterror.ClassOptimizer, mysql.ErrWindowFrameEndIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameEndIllegal])
	ErrWindowFrameIllegal              = terror.New(pterror.ClassOptimizer, mysql.ErrWindowFrameIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameIllegal])
	ErrWindowRangeFrameOrderType       = terror.New(pterror.ClassOptimizer, mysql.ErrWindowRangeFrameOrderType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameOrderType])
	ErrWindowRangeFrameTemporalType    = terror.New(pterror.ClassOptimizer, mysql.ErrWindowRangeFrameTemporalType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameTemporalType])
	ErrWindowRangeFrameNumericType     = terror.New(pterror.ClassOptimizer, mysql.ErrWindowRangeFrameNumericType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameNumericType])
	ErrWindowRangeBoundNotConstant     = terror.New(pterror.ClassOptimizer, mysql.ErrWindowRangeBoundNotConstant, mysql.MySQLErrName[mysql.ErrWindowRangeBoundNotConstant])
	ErrWindowRowsIntervalUse           = terror.New(pterror.ClassOptimizer, mysql.ErrWindowRowsIntervalUse, mysql.MySQLErrName[mysql.ErrWindowRowsIntervalUse])
	ErrWindowFunctionIgnoresFrame      = terror.New(pterror.ClassOptimizer, mysql.ErrWindowFunctionIgnoresFrame, mysql.MySQLErrName[mysql.ErrWindowFunctionIgnoresFrame])
	ErrUnsupportedOnGeneratedColumn    = terror.New(pterror.ClassOptimizer, mysql.ErrUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	ErrPrivilegeCheckFail              = terror.New(pterror.ClassOptimizer, mysql.ErrPrivilegeCheckFail, mysql.MySQLErrName[mysql.ErrPrivilegeCheckFail])
	ErrInvalidWildCard                 = terror.New(pterror.ClassOptimizer, mysql.ErrInvalidWildCard, mysql.MySQLErrName[mysql.ErrInvalidWildCard])
	ErrMixOfGroupFuncAndFields         = terror.New(pterror.ClassOptimizer, mysql.ErrMixOfGroupFuncAndFieldsIncompatible, mysql.MySQLErrName[mysql.ErrMixOfGroupFuncAndFieldsIncompatible])
	errTooBigPrecision                 = terror.New(pterror.ClassExpression, mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	ErrDBaccessDenied                  = terror.New(pterror.ClassOptimizer, mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied               = terror.New(pterror.ClassOptimizer, mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrSpecificAccessDenied            = terror.New(pterror.ClassOptimizer, mysql.ErrSpecificAccessDenied, mysql.MySQLErrName[mysql.ErrSpecificAccessDenied])
	ErrViewNoExplain                   = terror.New(pterror.ClassOptimizer, mysql.ErrViewNoExplain, mysql.MySQLErrName[mysql.ErrViewNoExplain])
	ErrWrongValueCountOnRow            = terror.New(pterror.ClassOptimizer, mysql.ErrWrongValueCountOnRow, mysql.MySQLErrName[mysql.ErrWrongValueCountOnRow])
	ErrViewInvalid                     = terror.New(pterror.ClassOptimizer, mysql.ErrViewInvalid, mysql.MySQLErrName[mysql.ErrViewInvalid])
	ErrNoSuchThread                    = terror.New(pterror.ClassOptimizer, mysql.ErrNoSuchThread, mysql.MySQLErrName[mysql.ErrNoSuchThread])
	ErrUnknownColumn                   = terror.New(pterror.ClassOptimizer, mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	ErrCartesianProductUnsupported     = terror.New(pterror.ClassOptimizer, mysql.ErrCartesianProductUnsupported, mysql.MySQLErrName[mysql.ErrCartesianProductUnsupported])
	ErrStmtNotFound                    = terror.New(pterror.ClassOptimizer, mysql.ErrPreparedStmtNotFound, mysql.MySQLErrName[mysql.ErrPreparedStmtNotFound])
	ErrAmbiguous                       = terror.New(pterror.ClassOptimizer, mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
	// Since we cannot know if user loggined with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = terror.New(pterror.ClassOptimizer, mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword])
)

func init() {
	mysqlErrCodeMap := map[pterror.ErrCode]uint16{
		mysql.ErrViewInvalid:                         mysql.ErrViewInvalid,
		mysql.ErrUnknown:                             mysql.ErrUnknown,
		mysql.ErrTablenameNotAllowedHere:             mysql.ErrTablenameNotAllowedHere,
		mysql.ErrUnsupportedType:                     mysql.ErrUnsupportedType,
		mysql.ErrAnalyzeMissIndex:                    mysql.ErrAnalyzeMissIndex,
		mysql.ErrWrongParamCount:                     mysql.ErrWrongParamCount,
		mysql.ErrSchemaChanged:                       mysql.ErrSchemaChanged,
		mysql.ErrNotSupportedYet:                     mysql.ErrNotSupportedYet,
		mysql.ErrWrongUsage:                          mysql.ErrWrongUsage,
		mysql.ErrUnknownTable:                        mysql.ErrUnknownTable,
		mysql.ErrWrongArguments:                      mysql.ErrWrongArguments,
		mysql.ErrBadGeneratedColumn:                  mysql.ErrBadGeneratedColumn,
		mysql.ErrFieldNotInGroupBy:                   mysql.ErrFieldNotInGroupBy,
		mysql.ErrBadTable:                            mysql.ErrBadTable,
		mysql.ErrKeyDoesNotExist:                     mysql.ErrKeyDoesNotExist,
		mysql.ErrOperandColumns:                      mysql.ErrOperandColumns,
		mysql.ErrInvalidGroupFuncUse:                 mysql.ErrInvalidGroupFuncUse,
		mysql.ErrIllegalReference:                    mysql.ErrIllegalReference,
		mysql.ErrNoDB:                                mysql.ErrNoDB,
		mysql.ErrUnknownExplainFormat:                mysql.ErrUnknownExplainFormat,
		mysql.ErrWrongGroupField:                     mysql.ErrWrongGroupField,
		mysql.ErrDupFieldName:                        mysql.ErrDupFieldName,
		mysql.ErrNonUpdatableTable:                   mysql.ErrNonUpdatableTable,
		mysql.ErrInternal:                            mysql.ErrInternal,
		mysql.ErrMixOfGroupFuncAndFieldsIncompatible: mysql.ErrMixOfGroupFuncAndFieldsIncompatible,
		mysql.ErrWrongNumberOfColumnsInSelect:        mysql.ErrWrongNumberOfColumnsInSelect,
		mysql.ErrWrongValueCountOnRow:                mysql.ErrWrongValueCountOnRow,
		mysql.ErrWindowInvalidWindowFuncUse:          mysql.ErrWindowInvalidWindowFuncUse,
		mysql.ErrWindowInvalidWindowFuncAliasUse:     mysql.ErrWindowInvalidWindowFuncAliasUse,
		mysql.ErrWindowNoSuchWindow:                  mysql.ErrWindowNoSuchWindow,
		mysql.ErrWindowCircularityInWindowGraph:      mysql.ErrWindowCircularityInWindowGraph,
		mysql.ErrWindowNoChildPartitioning:           mysql.ErrWindowNoChildPartitioning,
		mysql.ErrWindowNoInherentFrame:               mysql.ErrWindowNoInherentFrame,
		mysql.ErrWindowNoRedefineOrderBy:             mysql.ErrWindowNoRedefineOrderBy,
		mysql.ErrWindowDuplicateName:                 mysql.ErrWindowDuplicateName,
		mysql.ErrPartitionClauseOnNonpartitioned:     mysql.ErrPartitionClauseOnNonpartitioned,
		mysql.ErrDBaccessDenied:                      mysql.ErrDBaccessDenied,
		mysql.ErrTableaccessDenied:                   mysql.ErrTableaccessDenied,
		mysql.ErrSpecificAccessDenied:                mysql.ErrSpecificAccessDenied,
		mysql.ErrViewNoExplain:                       mysql.ErrViewNoExplain,
		mysql.ErrWindowFrameStartIllegal:             mysql.ErrWindowFrameStartIllegal,
		mysql.ErrWindowFrameEndIllegal:               mysql.ErrWindowFrameEndIllegal,
		mysql.ErrWindowFrameIllegal:                  mysql.ErrWindowFrameIllegal,
		mysql.ErrWindowRangeFrameOrderType:           mysql.ErrWindowRangeFrameOrderType,
		mysql.ErrWindowRangeFrameTemporalType:        mysql.ErrWindowRangeFrameTemporalType,
		mysql.ErrWindowRangeFrameNumericType:         mysql.ErrWindowRangeFrameNumericType,
		mysql.ErrWindowRangeBoundNotConstant:         mysql.ErrWindowRangeBoundNotConstant,
		mysql.ErrWindowRowsIntervalUse:               mysql.ErrWindowRowsIntervalUse,
		mysql.ErrWindowFunctionIgnoresFrame:          mysql.ErrWindowFunctionIgnoresFrame,
		mysql.ErrUnsupportedOnGeneratedColumn:        mysql.ErrUnsupportedOnGeneratedColumn,
		mysql.ErrNoSuchThread:                        mysql.ErrNoSuchThread,
		mysql.ErrAccessDenied:                        mysql.ErrAccessDenied,
		mysql.ErrPrivilegeCheckFail:                  mysql.ErrPrivilegeCheckFail,
		mysql.ErrCartesianProductUnsupported:         mysql.ErrCartesianProductUnsupported,
		mysql.ErrPreparedStmtNotFound:                mysql.ErrPreparedStmtNotFound,
		mysql.ErrNonUniq:                             mysql.ErrNonUniq,
		mysql.ErrBadField:                            mysql.ErrBadField,
		mysql.ErrNonuniqTable:                        mysql.ErrNonuniqTable,
		mysql.ErrTooBigPrecision:                     mysql.ErrTooBigPrecision,
		mysql.ErrInvalidWildCard:                     mysql.ErrInvalidWildCard,
	}
	terror.ErrClassToMySQLCodes[pterror.ClassOptimizer] = mysqlErrCodeMap
}
