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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount = terror.ClassExpression.New(mysql.ErrWrongParamcountToNativeFct, mysql.MySQLErrName[mysql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero          = terror.ClassExpression.New(mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])

	// All the un-exported errors are defined here:
	errFunctionNotExists             = terror.ClassExpression.New(mysql.ErrSpDoesNotExist, mysql.MySQLErrName[mysql.ErrSpDoesNotExist])
	errZlibZData                     = terror.ClassExpression.New(mysql.ErrZlibZData, mysql.MySQLErrName[mysql.ErrZlibZData])
	errZlibZBuf                      = terror.ClassExpression.New(mysql.ErrZlibZBuf, mysql.MySQLErrName[mysql.ErrZlibZBuf])
	errIncorrectArgs                 = terror.ClassExpression.New(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	errUnknownCharacterSet           = terror.ClassExpression.New(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	errDefaultValue                  = terror.ClassExpression.New(mysql.ErrInvalidDefault, "invalid default value")
	errDeprecatedSyntaxNoReplacement = terror.ClassExpression.New(mysql.ErrWarnDeprecatedSyntaxNoReplacement, mysql.MySQLErrName[mysql.ErrWarnDeprecatedSyntaxNoReplacement])
	errWarnAllowedPacketOverflowed   = terror.ClassExpression.New(mysql.ErrWarnAllowedPacketOverflowed, mysql.MySQLErrName[mysql.ErrWarnAllowedPacketOverflowed])
	errWarnOptionIgnored             = terror.ClassExpression.New(mysql.WarnOptionIgnored, mysql.MySQLErrName[mysql.WarnOptionIgnored])
)

func init() {
	expressionMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrWrongParamcountToNativeFct:        mysql.ErrWrongParamcountToNativeFct,
		mysql.ErrDivisionByZero:                    mysql.ErrDivisionByZero,
		mysql.ErrSpDoesNotExist:                    mysql.ErrSpDoesNotExist,
		mysql.ErrZlibZData:                         mysql.ErrZlibZData,
		mysql.ErrZlibZBuf:                          mysql.ErrZlibZBuf,
		mysql.ErrWrongArguments:                    mysql.ErrWrongArguments,
		mysql.ErrUnknownCharacterSet:               mysql.ErrUnknownCharacterSet,
		mysql.ErrInvalidDefault:                    mysql.ErrInvalidDefault,
		mysql.ErrWarnDeprecatedSyntaxNoReplacement: mysql.ErrWarnDeprecatedSyntaxNoReplacement,
		mysql.ErrWarnAllowedPacketOverflowed:       mysql.ErrWarnAllowedPacketOverflowed,
		mysql.WarnOptionIgnored:                    mysql.WarnOptionIgnored,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExpression] = expressionMySQLErrCodes
}

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(terror.ErrorEqual(err, types.ErrInvalidTimeFormat) || types.ErrIncorrectDatetimeValue.Equal(err)) {
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
