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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount     = terror.ClassExpression.New(mysql.ErrWrongParamcountToNativeFct, mysql.MySQLErrName[mysql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero              = terror.ClassExpression.New(mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	ErrRegexp                      = terror.ClassExpression.New(mysql.ErrRegexp, mysql.MySQLErrName[mysql.ErrRegexp])
	ErrOperandColumns              = terror.ClassExpression.New(mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrCutValueGroupConcat         = terror.ClassExpression.New(mysql.ErrCutValueGroupConcat, mysql.MySQLErrName[mysql.ErrCutValueGroupConcat])
	ErrFunctionsNoopImpl           = terror.ClassExpression.New(mysql.ErrNotSupportedYet, "function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
	ErrInvalidArgumentForLogarithm = terror.ClassExpression.New(mysql.ErrInvalidArgumentForLogarithm, mysql.MySQLErrName[mysql.ErrInvalidArgumentForLogarithm])
	ErrIncorrectType               = terror.ClassExpression.New(mysql.ErrIncorrectType, mysql.MySQLErrName[mysql.ErrIncorrectType])

	// All the un-exported errors are defined here:
	errFunctionNotExists             = terror.ClassExpression.New(mysql.ErrSpDoesNotExist, mysql.MySQLErrName[mysql.ErrSpDoesNotExist])
	errZlibZData                     = terror.ClassExpression.New(mysql.ErrZlibZData, mysql.MySQLErrName[mysql.ErrZlibZData])
	errZlibZBuf                      = terror.ClassExpression.New(mysql.ErrZlibZBuf, mysql.MySQLErrName[mysql.ErrZlibZBuf])
	errIncorrectArgs                 = terror.ClassExpression.New(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	errUnknownCharacterSet           = terror.ClassExpression.New(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	errDefaultValue                  = terror.ClassExpression.New(mysql.ErrInvalidDefault, "invalid default value")
	errDeprecatedSyntaxNoReplacement = terror.ClassExpression.New(mysql.ErrWarnDeprecatedSyntaxNoReplacement, mysql.MySQLErrName[mysql.ErrWarnDeprecatedSyntaxNoReplacement])
	errBadField                      = terror.ClassExpression.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	errWarnAllowedPacketOverflowed   = terror.ClassExpression.New(mysql.ErrWarnAllowedPacketOverflowed, mysql.MySQLErrName[mysql.ErrWarnAllowedPacketOverflowed])
	errWarnOptionIgnored             = terror.ClassExpression.New(mysql.WarnOptionIgnored, mysql.MySQLErrName[mysql.WarnOptionIgnored])
	errTruncatedWrongValue           = terror.ClassExpression.New(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrTruncatedWrongValue])
	errUnknownLocale                 = terror.ClassExpression.New(mysql.ErrUnknownLocale, mysql.MySQLErrName[mysql.ErrUnknownLocale])
	errNonUniq                       = terror.ClassExpression.New(mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])

	// Sequence usage privilege check.
	errSequenceAccessDenied = terror.ClassExpression.New(mysql.ErrTableaccessDenied, mysql.MySQLErrName[mysql.ErrTableaccessDenied])
)

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
