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

package plan

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

const (
	codeUnsupportedType  terror.ErrCode = 1
	codeAnalyzeMissIndex                = 2
	codeUnsupported                     = 3
	codeStmtNotFound                    = 4
	codeWrongParamCount                 = 5
	codeSchemaChanged                   = 6

	codeWrongUsage                   = mysql.ErrWrongUsage
	codeAmbiguous                    = mysql.ErrNonUniq
	codeUnknownColumn                = mysql.ErrBadField
	codeUnknownTable                 = mysql.ErrUnknownTable
	codeWrongArguments               = mysql.ErrWrongArguments
	codeBadGeneratedColumn           = mysql.ErrBadGeneratedColumn
	codeFieldNotInGroupBy            = mysql.ErrFieldNotInGroupBy
	codeBadTable                     = mysql.ErrBadTable
	codeKeyDoesNotExist              = mysql.ErrKeyDoesNotExist
	codeOperandColumns               = mysql.ErrOperandColumns
	codeInvalidWildCard              = mysql.ErrParse
	codeInvalidGroupFuncUse          = mysql.ErrInvalidGroupFuncUse
	codeIllegalReference             = mysql.ErrIllegalReference
	codeNoDB                         = mysql.ErrNoDB
	codeUnknownExplainFormat         = mysql.ErrUnknownExplainFormat
	codeWrongGroupField              = mysql.ErrWrongGroupField
	codeDupFieldName                 = mysql.ErrDupFieldName
	codeNonUpdatableTable            = mysql.ErrNonUpdatableTable
	codeInternal                     = mysql.ErrInternal
	codeMixOfGroupFuncAndFields      = mysql.ErrMixOfGroupFuncAndFields
	codeNonUniqTable                 = mysql.ErrNonuniqTable
	codeWrongNumberOfColumnsInSelect = mysql.ErrWrongNumberOfColumnsInSelect
	codeWrongValueCountOnRow         = mysql.ErrWrongValueCountOnRow
)

// error definitions.
var (
	ErrUnsupportedType             = terror.ClassOptimizer.New(codeUnsupportedType, "Unsupported type %T")
	ErrAnalyzeMissIndex            = terror.ClassOptimizer.New(codeAnalyzeMissIndex, "Index '%s' in field list does not exist in table '%s'")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(codeUnsupported, "Cartesian product is unsupported")
	ErrStmtNotFound                = terror.ClassOptimizer.New(codeStmtNotFound, "Prepared statement not found")
	ErrWrongParamCount             = terror.ClassOptimizer.New(codeWrongParamCount, "Wrong parameter count")
	ErrSchemaChanged               = terror.ClassOptimizer.New(codeSchemaChanged, "Schema has changed")

	ErrWrongUsage                   = terror.ClassOptimizer.New(codeWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	ErrAmbiguous                    = terror.ClassOptimizer.New(codeAmbiguous, mysql.MySQLErrName[mysql.ErrNonUniq])
	ErrUnknownColumn                = terror.ClassOptimizer.New(codeUnknownColumn, mysql.MySQLErrName[mysql.ErrBadField])
	ErrUnknownTable                 = terror.ClassOptimizer.New(codeUnknownTable, mysql.MySQLErrName[mysql.ErrUnknownTable])
	ErrWrongArguments               = terror.ClassOptimizer.New(codeWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	ErrWrongNumberOfColumnsInSelect = terror.ClassOptimizer.New(codeWrongNumberOfColumnsInSelect, mysql.MySQLErrName[mysql.ErrWrongNumberOfColumnsInSelect])
	ErrBadGeneratedColumn           = terror.ClassOptimizer.New(codeBadGeneratedColumn, mysql.MySQLErrName[mysql.ErrBadGeneratedColumn])
	ErrFieldNotInGroupBy            = terror.ClassOptimizer.New(codeFieldNotInGroupBy, mysql.MySQLErrName[mysql.ErrFieldNotInGroupBy])
	ErrBadTable                     = terror.ClassOptimizer.New(codeBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	ErrKeyDoesNotExist              = terror.ClassOptimizer.New(codeKeyDoesNotExist, mysql.MySQLErrName[mysql.ErrKeyDoesNotExist])
	ErrOperandColumns               = terror.ClassOptimizer.New(codeOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrInvalidWildCard              = terror.ClassOptimizer.New(codeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrInvalidGroupFuncUse          = terror.ClassOptimizer.New(codeInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	ErrIllegalReference             = terror.ClassOptimizer.New(codeIllegalReference, mysql.MySQLErrName[mysql.ErrIllegalReference])
	ErrNoDB                         = terror.ClassOptimizer.New(codeNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
	ErrUnknownExplainFormat         = terror.ClassOptimizer.New(codeUnknownExplainFormat, mysql.MySQLErrName[mysql.ErrUnknownExplainFormat])
	ErrWrongGroupField              = terror.ClassOptimizer.New(codeWrongGroupField, mysql.MySQLErrName[mysql.ErrWrongGroupField])
	ErrDupFieldName                 = terror.ClassOptimizer.New(codeDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	ErrNonUpdatableTable            = terror.ClassOptimizer.New(codeNonUpdatableTable, mysql.MySQLErrName[mysql.ErrNonUpdatableTable])
	ErrInternal                     = terror.ClassOptimizer.New(codeInternal, mysql.MySQLErrName[mysql.ErrInternal])
	ErrMixOfGroupFuncAndFields      = terror.ClassOptimizer.New(codeMixOfGroupFuncAndFields, "In aggregated query without GROUP BY, expression #%d of SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by")
	ErrNonUniqTable                 = terror.ClassOptimizer.New(codeNonUniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	ErrWrongValueCountOnRow         = terror.ClassOptimizer.New(mysql.ErrWrongValueCountOnRow, mysql.MySQLErrName[mysql.ErrWrongValueCountOnRow])
)

func init() {
	mysqlErrCodeMap := map[terror.ErrCode]uint16{
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
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mysqlErrCodeMap
}
