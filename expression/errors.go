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
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount     = dbterror.ClassExpression.NewStd(mysql.ErrWrongParamcountToNativeFct)
	ErrDivisionByZero              = dbterror.ClassExpression.NewStd(mysql.ErrDivisionByZero)
	ErrRegexp                      = dbterror.ClassExpression.NewStd(mysql.ErrRegexp)
	ErrOperandColumns              = dbterror.ClassExpression.NewStd(mysql.ErrOperandColumns)
	ErrCutValueGroupConcat         = dbterror.ClassExpression.NewStd(mysql.ErrCutValueGroupConcat)
	ErrFunctionsNoopImpl           = dbterror.ClassExpression.NewStdErr(mysql.ErrNotSupportedYet, pmysql.Message("function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", nil))
	ErrInvalidArgumentForLogarithm = dbterror.ClassExpression.NewStd(mysql.ErrInvalidArgumentForLogarithm)
	ErrIncorrectType               = dbterror.ClassExpression.NewStd(mysql.ErrIncorrectType)
	ErrInvalidTableSample          = dbterror.ClassExpression.NewStd(mysql.ErrInvalidTableSample)

	// All the un-exported errors are defined here:
	errFunctionNotExists             = dbterror.ClassExpression.NewStd(mysql.ErrSpDoesNotExist)
	errZlibZData                     = dbterror.ClassExpression.NewStd(mysql.ErrZlibZData)
	errZlibZBuf                      = dbterror.ClassExpression.NewStd(mysql.ErrZlibZBuf)
	errIncorrectArgs                 = dbterror.ClassExpression.NewStd(mysql.ErrWrongArguments)
	errUnknownCharacterSet           = dbterror.ClassExpression.NewStd(mysql.ErrUnknownCharacterSet)
	errDefaultValue                  = dbterror.ClassExpression.NewStdErr(mysql.ErrInvalidDefault, pmysql.Message("invalid default value", nil))
	errDeprecatedSyntaxNoReplacement = dbterror.ClassExpression.NewStd(mysql.ErrWarnDeprecatedSyntaxNoReplacement)
	errBadField                      = dbterror.ClassExpression.NewStd(mysql.ErrBadField)
	errWarnAllowedPacketOverflowed   = dbterror.ClassExpression.NewStd(mysql.ErrWarnAllowedPacketOverflowed)
	errWarnOptionIgnored             = dbterror.ClassExpression.NewStd(mysql.WarnOptionIgnored)
	errTruncatedWrongValue           = dbterror.ClassExpression.NewStd(mysql.ErrTruncatedWrongValue)
	errUnknownLocale                 = dbterror.ClassExpression.NewStd(mysql.ErrUnknownLocale)
	errNonUniq                       = dbterror.ClassExpression.NewStd(mysql.ErrNonUniq)

	// Sequence usage privilege check.
	errSequenceAccessDenied = dbterror.ClassExpression.NewStd(mysql.ErrTableaccessDenied)
)

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) || types.ErrWrongValueForType.Equal(err) ||
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
