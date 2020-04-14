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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

const (
	codeUnsupportedType  terror.ErrCode = 1
	codeAnalyzeMissIndex                = 2
	codeUnsupported                     = 3
	codeStmtNotFound                    = 4
	codeWrongParamCount                 = 5
	codeSchemaChanged                   = 6

	codeNotSupportedYet                 = mysql.ErrNotSupportedYet
	codeWrongUsage                      = mysql.ErrWrongUsage
	codeAmbiguous                       = mysql.ErrNonUniq
	codeUnknown                         = mysql.ErrUnknown
	codeUnknownColumn                   = mysql.ErrBadField
	codeUnknownTable                    = mysql.ErrUnknownTable
	codeWrongArguments                  = mysql.ErrWrongArguments
	codeBadGeneratedColumn              = mysql.ErrBadGeneratedColumn
	codeFieldNotInGroupBy               = mysql.ErrFieldNotInGroupBy
	codeBadTable                        = mysql.ErrBadTable
	codeKeyDoesNotExist                 = mysql.ErrKeyDoesNotExist
	codeOperandColumns                  = mysql.ErrOperandColumns
	codeInvalidWildCard                 = mysql.ErrParse
	codeInvalidGroupFuncUse             = mysql.ErrInvalidGroupFuncUse
	codeIllegalReference                = mysql.ErrIllegalReference
	codeNoDB                            = mysql.ErrNoDB
	codeUnknownExplainFormat            = mysql.ErrUnknownExplainFormat
	codeWrongGroupField                 = mysql.ErrWrongGroupField
	codeDupFieldName                    = mysql.ErrDupFieldName
	codeNonUpdatableTable               = mysql.ErrNonUpdatableTable
	codeInternal                        = mysql.ErrInternal
	codeMixOfGroupFuncAndFields         = mysql.ErrMixOfGroupFuncAndFields
	codeNonUniqTable                    = mysql.ErrNonuniqTable
	codeWrongNumberOfColumnsInSelect    = mysql.ErrWrongNumberOfColumnsInSelect
	codeWrongValueCountOnRow            = mysql.ErrWrongValueCountOnRow
	codeTablenameNotAllowedHere         = mysql.ErrTablenameNotAllowedHere
	codePrivilegeCheckFail              = mysql.ErrUnknown
	codeWindowInvalidWindowFuncUse      = mysql.ErrWindowInvalidWindowFuncUse
	codeWindowInvalidWindowFuncAliasUse = mysql.ErrWindowInvalidWindowFuncAliasUse
	codeWindowNoSuchWindow              = mysql.ErrWindowNoSuchWindow
	codeWindowCircularityInWindowGraph  = mysql.ErrWindowCircularityInWindowGraph
	codeWindowNoChildPartitioning       = mysql.ErrWindowNoChildPartitioning
	codeWindowNoInherentFrame           = mysql.ErrWindowNoInherentFrame
	codeWindowNoRedefineOrderBy         = mysql.ErrWindowNoRedefineOrderBy
	codeWindowDuplicateName             = mysql.ErrWindowDuplicateName
	codeErrTooBigPrecision              = mysql.ErrTooBigPrecision
	codePartitionClauseOnNonpartitioned = mysql.ErrPartitionClauseOnNonpartitioned
	codeDBaccessDenied                  = mysql.ErrDBaccessDenied
	codeTableaccessDenied               = mysql.ErrTableaccessDenied
	codeSpecificAccessDenied            = mysql.ErrSpecificAccessDenied
	codeViewNoExplain                   = mysql.ErrViewNoExplain
	codeWindowFrameStartIllegal         = mysql.ErrWindowFrameStartIllegal
	codeWindowFrameEndIllegal           = mysql.ErrWindowFrameEndIllegal
	codeWindowFrameIllegal              = mysql.ErrWindowFrameIllegal
	codeWindowRangeFrameOrderType       = mysql.ErrWindowRangeFrameOrderType
	codeWindowRangeFrameTemporalType    = mysql.ErrWindowRangeFrameTemporalType
	codeWindowRangeFrameNumericType     = mysql.ErrWindowRangeFrameNumericType
	codeWindowRangeBoundNotConstant     = mysql.ErrWindowRangeBoundNotConstant
	codeWindowRowsIntervalUse           = mysql.ErrWindowRowsIntervalUse
	codeWindowFunctionIgnoresFrame      = mysql.ErrWindowFunctionIgnoresFrame
	codeUnsupportedOnGeneratedColumn    = mysql.ErrUnsupportedOnGeneratedColumn
)

// error definitions.
var (
	ErrUnsupportedType             = terror.ClassOptimizer.New(codeUnsupportedType, "Unsupported type %T")
	ErrAnalyzeMissIndex            = terror.ClassOptimizer.New(codeAnalyzeMissIndex, "Index '%s' in field list does not exist in table '%s'")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(codeUnsupported, "Cartesian product is unsupported")
	ErrStmtNotFound                = terror.ClassOptimizer.New(codeStmtNotFound, "Prepared statement not found")
	ErrWrongParamCount             = terror.ClassOptimizer.New(codeWrongParamCount, "Wrong parameter count")
	ErrSchemaChanged               = terror.ClassOptimizer.New(codeSchemaChanged, "Schema has changed")
	ErrTablenameNotAllowedHere     = terror.ClassOptimizer.New(codeTablenameNotAllowedHere, "Table '%s' from one of the %ss cannot be used in %s")

	ErrNotSupportedYet                 = terror.ClassOptimizer.New(codeNotSupportedYet, mysql.MySQLErrName[mysql.ErrNotSupportedYet])
	ErrWrongUsage                      = terror.ClassOptimizer.New(codeWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	ErrAmbiguous                       = terror.ClassOptimizer.New(codeAmbiguous, mysql.MySQLErrName[mysql.ErrNonUniq])
	ErrUnknown                         = terror.ClassOptimizer.New(codeUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
	ErrUnknownColumn                   = terror.ClassOptimizer.New(codeUnknownColumn, mysql.MySQLErrName[mysql.ErrBadField])
	ErrUnknownTable                    = terror.ClassOptimizer.New(codeUnknownTable, mysql.MySQLErrName[mysql.ErrUnknownTable])
	ErrWrongArguments                  = terror.ClassOptimizer.New(codeWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	ErrWrongNumberOfColumnsInSelect    = terror.ClassOptimizer.New(codeWrongNumberOfColumnsInSelect, mysql.MySQLErrName[mysql.ErrWrongNumberOfColumnsInSelect])
	ErrBadGeneratedColumn              = terror.ClassOptimizer.New(codeBadGeneratedColumn, mysql.MySQLErrName[mysql.ErrBadGeneratedColumn])
	ErrFieldNotInGroupBy               = terror.ClassOptimizer.New(codeFieldNotInGroupBy, mysql.MySQLErrName[mysql.ErrFieldNotInGroupBy])
	ErrBadTable                        = terror.ClassOptimizer.New(codeBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	ErrKeyDoesNotExist                 = terror.ClassOptimizer.New(codeKeyDoesNotExist, mysql.MySQLErrName[mysql.ErrKeyDoesNotExist])
	ErrOperandColumns                  = terror.ClassOptimizer.New(codeOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrInvalidWildCard                 = terror.ClassOptimizer.New(codeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrInvalidGroupFuncUse             = terror.ClassOptimizer.New(codeInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	ErrIllegalReference                = terror.ClassOptimizer.New(codeIllegalReference, mysql.MySQLErrName[mysql.ErrIllegalReference])
	ErrNoDB                            = terror.ClassOptimizer.New(codeNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
	ErrUnknownExplainFormat            = terror.ClassOptimizer.New(codeUnknownExplainFormat, mysql.MySQLErrName[mysql.ErrUnknownExplainFormat])
	ErrWrongGroupField                 = terror.ClassOptimizer.New(codeWrongGroupField, mysql.MySQLErrName[mysql.ErrWrongGroupField])
	ErrDupFieldName                    = terror.ClassOptimizer.New(codeDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	ErrNonUpdatableTable               = terror.ClassOptimizer.New(codeNonUpdatableTable, mysql.MySQLErrName[mysql.ErrNonUpdatableTable])
	ErrInternal                        = terror.ClassOptimizer.New(codeInternal, mysql.MySQLErrName[mysql.ErrInternal])
	ErrMixOfGroupFuncAndFields         = terror.ClassOptimizer.New(codeMixOfGroupFuncAndFields, "In aggregated query without GROUP BY, expression #%d of SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by")
	ErrNonUniqTable                    = terror.ClassOptimizer.New(codeNonUniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	ErrWrongValueCountOnRow            = terror.ClassOptimizer.New(mysql.ErrWrongValueCountOnRow, mysql.MySQLErrName[mysql.ErrWrongValueCountOnRow])
	ErrViewInvalid                     = terror.ClassOptimizer.New(mysql.ErrViewInvalid, mysql.MySQLErrName[mysql.ErrViewInvalid])
	ErrPrivilegeCheckFail              = terror.ClassOptimizer.New(codePrivilegeCheckFail, "privilege check fail")
	ErrWindowInvalidWindowFuncUse      = terror.ClassOptimizer.New(codeWindowInvalidWindowFuncUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncUse])
	ErrWindowInvalidWindowFuncAliasUse = terror.ClassOptimizer.New(codeWindowInvalidWindowFuncAliasUse, mysql.MySQLErrName[mysql.ErrWindowInvalidWindowFuncAliasUse])
	ErrWindowNoSuchWindow              = terror.ClassOptimizer.New(codeWindowNoSuchWindow, mysql.MySQLErrName[mysql.ErrWindowNoSuchWindow])
	ErrWindowCircularityInWindowGraph  = terror.ClassOptimizer.New(codeWindowCircularityInWindowGraph, mysql.MySQLErrName[mysql.ErrWindowCircularityInWindowGraph])
	ErrWindowNoChildPartitioning       = terror.ClassOptimizer.New(codeWindowNoChildPartitioning, mysql.MySQLErrName[mysql.ErrWindowNoChildPartitioning])
	ErrWindowNoInherentFrame           = terror.ClassOptimizer.New(codeWindowNoInherentFrame, mysql.MySQLErrName[mysql.ErrWindowNoInherentFrame])
	ErrWindowNoRedefineOrderBy         = terror.ClassOptimizer.New(codeWindowNoRedefineOrderBy, mysql.MySQLErrName[mysql.ErrWindowNoRedefineOrderBy])
	ErrWindowDuplicateName             = terror.ClassOptimizer.New(codeWindowDuplicateName, mysql.MySQLErrName[mysql.ErrWindowDuplicateName])
	ErrPartitionClauseOnNonpartitioned = terror.ClassOptimizer.New(codePartitionClauseOnNonpartitioned, mysql.MySQLErrName[mysql.ErrPartitionClauseOnNonpartitioned])
	ErrNoSuchTable                     = terror.ClassOptimizer.New(mysql.ErrNoSuchTable, mysql.MySQLErrName[mysql.ErrNoSuchTable])
	errTooBigPrecision                 = terror.ClassExpression.New(mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	ErrDBaccessDenied                  = terror.ClassOptimizer.New(mysql.ErrDBaccessDenied, mysql.MySQLErrName[mysql.ErrDBaccessDenied])
	ErrTableaccessDenied               = terror.ClassOptimizer.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
	ErrSpecificAccessDenied            = terror.ClassOptimizer.New(mysql.ErrSpecificAccessDenied, mysql.MySQLErrName[mysql.ErrSpecificAccessDenied])
	ErrViewNoExplain                   = terror.ClassOptimizer.New(mysql.ErrViewNoExplain, mysql.MySQLErrName[mysql.ErrViewNoExplain])
	ErrWindowFrameStartIllegal         = terror.ClassOptimizer.New(codeWindowFrameStartIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameStartIllegal])
	ErrWindowFrameEndIllegal           = terror.ClassOptimizer.New(codeWindowFrameEndIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameEndIllegal])
	ErrWindowFrameIllegal              = terror.ClassOptimizer.New(codeWindowFrameIllegal, mysql.MySQLErrName[mysql.ErrWindowFrameIllegal])
	ErrWindowRangeFrameOrderType       = terror.ClassOptimizer.New(codeWindowRangeFrameOrderType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameOrderType])
	ErrWindowRangeFrameTemporalType    = terror.ClassOptimizer.New(codeWindowRangeFrameTemporalType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameTemporalType])
	ErrWindowRangeFrameNumericType     = terror.ClassOptimizer.New(codeWindowRangeFrameNumericType, mysql.MySQLErrName[mysql.ErrWindowRangeFrameNumericType])
	ErrWindowRangeBoundNotConstant     = terror.ClassOptimizer.New(codeWindowRangeBoundNotConstant, mysql.MySQLErrName[mysql.ErrWindowRangeBoundNotConstant])
	ErrWindowRowsIntervalUse           = terror.ClassOptimizer.New(codeWindowRowsIntervalUse, mysql.MySQLErrName[mysql.ErrWindowRowsIntervalUse])
	ErrWindowFunctionIgnoresFrame      = terror.ClassOptimizer.New(codeWindowFunctionIgnoresFrame, mysql.MySQLErrName[mysql.ErrWindowFunctionIgnoresFrame])
	ErrUnsupportedOnGeneratedColumn    = terror.ClassOptimizer.New(codeUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	ErrNoSuchThread                    = terror.ClassOptimizer.New(mysql.ErrNoSuchThread, mysql.MySQLErrName[mysql.ErrNoSuchThread])
	// Since we cannot know if user loggined with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = terror.ClassOptimizer.New(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDeniedNoPassword])
)

func init() {
	mysqlErrCodeMap := map[terror.ErrCode]uint16{
		codeNotSupportedYet:              mysql.ErrNotSupportedYet,
		codeWrongUsage:                   mysql.ErrWrongUsage,
		codeAmbiguous:                    mysql.ErrNonUniq,
		codeUnknownColumn:                mysql.ErrBadField,
		codeUnknownTable:                 mysql.ErrBadTable,
		codeWrongArguments:               mysql.ErrWrongArguments,
		codeBadGeneratedColumn:           mysql.ErrBadGeneratedColumn,
		codeFieldNotInGroupBy:            mysql.ErrFieldNotInGroupBy,
		codeBadTable:                     mysql.ErrBadTable,
		codeKeyDoesNotExist:              mysql.ErrKeyDoesNotExist,
		codeOperandColumns:               mysql.ErrOperandColumns,
		codeInvalidWildCard:              mysql.ErrParse,
		codeInvalidGroupFuncUse:          mysql.ErrInvalidGroupFuncUse,
		codeIllegalReference:             mysql.ErrIllegalReference,
		codeNoDB:                         mysql.ErrNoDB,
		codeUnknownExplainFormat:         mysql.ErrUnknownExplainFormat,
		codeWrongGroupField:              mysql.ErrWrongGroupField,
		codeDupFieldName:                 mysql.ErrDupFieldName,
		codeNonUpdatableTable:            mysql.ErrUnknownTable,
		codeInternal:                     mysql.ErrInternal,
		codeMixOfGroupFuncAndFields:      mysql.ErrMixOfGroupFuncAndFields,
		codeNonUniqTable:                 mysql.ErrNonuniqTable,
		codeWrongNumberOfColumnsInSelect: mysql.ErrWrongNumberOfColumnsInSelect,
		codeWrongValueCountOnRow:         mysql.ErrWrongValueCountOnRow,

		codeWindowInvalidWindowFuncUse:      mysql.ErrWindowInvalidWindowFuncUse,
		codeWindowInvalidWindowFuncAliasUse: mysql.ErrWindowInvalidWindowFuncAliasUse,
		codeWindowNoSuchWindow:              mysql.ErrWindowNoSuchWindow,
		codeWindowCircularityInWindowGraph:  mysql.ErrWindowCircularityInWindowGraph,
		codeWindowNoChildPartitioning:       mysql.ErrWindowNoChildPartitioning,
		codeWindowNoInherentFrame:           mysql.ErrWindowNoInherentFrame,
		codeWindowNoRedefineOrderBy:         mysql.ErrWindowNoRedefineOrderBy,
		codeWindowDuplicateName:             mysql.ErrWindowDuplicateName,
		codePartitionClauseOnNonpartitioned: mysql.ErrPartitionClauseOnNonpartitioned,
		codeErrTooBigPrecision:              mysql.ErrTooBigPrecision,
		codeDBaccessDenied:                  mysql.ErrDBaccessDenied,
		codeTableaccessDenied:               mysql.ErrTableaccessDenied,
		codeSpecificAccessDenied:            mysql.ErrSpecificAccessDenied,
		codeViewNoExplain:                   mysql.ErrViewNoExplain,
		codeWindowFrameStartIllegal:         mysql.ErrWindowFrameStartIllegal,
		codeWindowFrameEndIllegal:           mysql.ErrWindowFrameEndIllegal,
		codeWindowFrameIllegal:              mysql.ErrWindowFrameIllegal,
		codeWindowRangeFrameOrderType:       mysql.ErrWindowRangeFrameOrderType,
		codeWindowRangeFrameTemporalType:    mysql.ErrWindowRangeFrameTemporalType,
		codeWindowRangeFrameNumericType:     mysql.ErrWindowRangeFrameNumericType,
		codeWindowRangeBoundNotConstant:     mysql.ErrWindowRangeBoundNotConstant,
		codeWindowRowsIntervalUse:           mysql.ErrWindowRowsIntervalUse,
		codeWindowFunctionIgnoresFrame:      mysql.ErrWindowFunctionIgnoresFrame,
		codeUnsupportedOnGeneratedColumn:    mysql.ErrUnsupportedOnGeneratedColumn,

		mysql.ErrNoSuchThread: mysql.ErrNoSuchThread,
		mysql.ErrAccessDenied: mysql.ErrAccessDenied,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mysqlErrCodeMap
}
