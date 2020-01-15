// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	pterror "github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount = terror.New(pterror.ClassExpression, mysql.ErrWrongParamcountToNativeFct, mysql.MySQLErrName[mysql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero          = terror.New(pterror.ClassExpression, mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	ErrRegexp                  = terror.New(pterror.ClassExpression, mysql.ErrRegexp, mysql.MySQLErrName[mysql.ErrRegexp])
	ErrOperandColumns          = terror.New(pterror.ClassExpression, mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrCutValueGroupConcat     = terror.New(pterror.ClassExpression, mysql.ErrCutValueGroupConcat, mysql.MySQLErrName[mysql.ErrCutValueGroupConcat])
	ErrFunctionsNoopImpl       = terror.New(pterror.ClassExpression, mysql.ErrNotSupportedYet, "function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
	ErrIncorrectType           = terror.New(pterror.ClassExpression, mysql.ErrIncorrectType, mysql.MySQLErrName[mysql.ErrIncorrectType])

	// All the un-exported errors are defined here:
	errFunctionNotExists             = terror.New(pterror.ClassExpression, mysql.ErrSpDoesNotExist, mysql.MySQLErrName[mysql.ErrSpDoesNotExist])
	errZlibZData                     = terror.New(pterror.ClassExpression, mysql.ErrZlibZData, mysql.MySQLErrName[mysql.ErrZlibZData])
	errZlibZBuf                      = terror.New(pterror.ClassExpression, mysql.ErrZlibZBuf, mysql.MySQLErrName[mysql.ErrZlibZBuf])
	errIncorrectArgs                 = terror.New(pterror.ClassExpression, mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	errUnknownCharacterSet           = terror.New(pterror.ClassExpression, mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	errDefaultValue                  = terror.New(pterror.ClassExpression, mysql.ErrInvalidDefault, "invalid default value")
	errDeprecatedSyntaxNoReplacement = terror.New(pterror.ClassExpression, mysql.ErrWarnDeprecatedSyntaxNoReplacement, mysql.MySQLErrName[mysql.ErrWarnDeprecatedSyntaxNoReplacement])
	errBadField                      = terror.New(pterror.ClassExpression, mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	errWarnAllowedPacketOverflowed   = terror.New(pterror.ClassExpression, mysql.ErrWarnAllowedPacketOverflowed, mysql.MySQLErrName[mysql.ErrWarnAllowedPacketOverflowed])
	errWarnOptionIgnored             = terror.New(pterror.ClassExpression, mysql.WarnOptionIgnored, mysql.MySQLErrName[mysql.WarnOptionIgnored])
	errTruncatedWrongValue           = terror.New(pterror.ClassExpression, mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrTruncatedWrongValue])
	errUnknownLocale                 = terror.New(pterror.ClassExpression, mysql.ErrUnknownLocale, mysql.MySQLErrName[mysql.ErrUnknownLocale])
	errNonUniq                       = terror.New(pterror.ClassExpression, mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
)

func init() {
	expressionMySQLErrCodes := map[pterror.ErrCode]uint16{
		mysql.ErrWrongParamcountToNativeFct:        mysql.ErrWrongParamcountToNativeFct,
		mysql.ErrDivisionByZero:                    mysql.ErrDivisionByZero,
		mysql.ErrSpDoesNotExist:                    mysql.ErrSpDoesNotExist,
		mysql.ErrNotSupportedYet:                   mysql.ErrNotSupportedYet,
		mysql.ErrZlibZData:                         mysql.ErrZlibZData,
		mysql.ErrZlibZBuf:                          mysql.ErrZlibZBuf,
		mysql.ErrWrongArguments:                    mysql.ErrWrongArguments,
		mysql.ErrUnknownCharacterSet:               mysql.ErrUnknownCharacterSet,
		mysql.ErrInvalidDefault:                    mysql.ErrInvalidDefault,
		mysql.ErrWarnDeprecatedSyntaxNoReplacement: mysql.ErrWarnDeprecatedSyntaxNoReplacement,
		mysql.ErrOperandColumns:                    mysql.ErrOperandColumns,
		mysql.ErrCutValueGroupConcat:               mysql.ErrCutValueGroupConcat,
		mysql.ErrRegexp:                            mysql.ErrRegexp,
		mysql.ErrWarnAllowedPacketOverflowed:       mysql.ErrWarnAllowedPacketOverflowed,
		mysql.WarnOptionIgnored:                    mysql.WarnOptionIgnored,
		mysql.ErrTruncatedWrongValue:               mysql.ErrTruncatedWrongValue,
		mysql.ErrUnknownLocale:                     mysql.ErrUnknownLocale,
		mysql.ErrBadField:                          mysql.ErrBadField,
		mysql.ErrNonUniq:                           mysql.ErrNonUniq,
		mysql.ErrIncorrectType:                     mysql.ErrIncorrectType,
	}
	terror.ErrClassToMySQLCodes[pterror.ClassExpression] = expressionMySQLErrCodes
}

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) ||
		types.ErrTruncatedWrongVal.Equal(err) || types.ErrInvalidWeekModeFormat.Equal(err) ||
		types.ErrDatetimeFunctionOverflow.Equal(err)) {
		return err
	}
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StrictSQLMode && (sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt) {
		return err
	}
	sc.AppendWarning(err)
	return nil
}

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx sessionctx.Context) error {
	sc := ctx.GetSessionVars().StmtCtx
	if sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt {
		if !ctx.GetSessionVars().SQLMode.HasErrorForDivisionByZeroMode() {
			return nil
		}
		if ctx.GetSessionVars().StrictSQLMode && !sc.DividedByZeroAsWarning {
			return ErrDivisionByZero
		}
	}
	sc.AppendWarning(ErrDivisionByZero)
	return nil
}
