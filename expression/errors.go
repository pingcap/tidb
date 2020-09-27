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
	pmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount     = terror.ClassExpression.NewStd(mysql.ErrWrongParamcountToNativeFct)
	ErrDivisionByZero              = terror.ClassExpression.NewStd(mysql.ErrDivisionByZero)
	ErrRegexp                      = terror.ClassExpression.NewStd(mysql.ErrRegexp)
	ErrOperandColumns              = terror.ClassExpression.NewStd(mysql.ErrOperandColumns)
	ErrCutValueGroupConcat         = terror.ClassExpression.NewStd(mysql.ErrCutValueGroupConcat)
	ErrFunctionsNoopImpl           = terror.ClassExpression.NewStdErr(mysql.ErrNotSupportedYet, pmysql.Message("function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", nil), "", "")
	ErrInvalidArgumentForLogarithm = terror.ClassExpression.NewStd(mysql.ErrInvalidArgumentForLogarithm)
	ErrIncorrectType               = terror.ClassExpression.NewStd(mysql.ErrIncorrectType)

	// All the un-exported errors are defined here:
	errFunctionNotExists             = terror.ClassExpression.NewStd(mysql.ErrSpDoesNotExist)
	errZlibZData                     = terror.ClassExpression.NewStd(mysql.ErrZlibZData)
	errZlibZBuf                      = terror.ClassExpression.NewStd(mysql.ErrZlibZBuf)
	errIncorrectArgs                 = terror.ClassExpression.NewStd(mysql.ErrWrongArguments)
	errUnknownCharacterSet           = terror.ClassExpression.NewStd(mysql.ErrUnknownCharacterSet)
	errDefaultValue                  = terror.ClassExpression.NewStdErr(mysql.ErrInvalidDefault, pmysql.Message("invalid default value", nil), "", "")
	errDeprecatedSyntaxNoReplacement = terror.ClassExpression.NewStd(mysql.ErrWarnDeprecatedSyntaxNoReplacement)
	errBadField                      = terror.ClassExpression.NewStd(mysql.ErrBadField)
	errWarnAllowedPacketOverflowed   = terror.ClassExpression.NewStd(mysql.ErrWarnAllowedPacketOverflowed)
	errWarnOptionIgnored             = terror.ClassExpression.NewStd(mysql.WarnOptionIgnored)
	errTruncatedWrongValue           = terror.ClassExpression.NewStd(mysql.ErrTruncatedWrongValue)
	errUnknownLocale                 = terror.ClassExpression.NewStd(mysql.ErrUnknownLocale)
	errNonUniq                       = terror.ClassExpression.NewStd(mysql.ErrNonUniq)

	// Sequence usage privilege check.
	errSequenceAccessDenied = terror.ClassExpression.NewStd(mysql.ErrTableaccessDenied)
)

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) ||
		types.ErrTruncatedWrongVal.Equal(err) || types.ErrInvalidWeekModeFormat.Equal(err) ||
		types.ErrDatetimeFunctionOverflow.Equal(err)) {
		return err
	}
	sc := ctx.GetSessionVars().StmtCtx
	err = sc.HandleTruncate(err)
	if ctx.GetSessionVars().StrictSQLMode && (sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt) {
		return err
	}
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
