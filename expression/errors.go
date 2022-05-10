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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	mysql "github.com/pingcap/tidb/errno"
	pmysql "github.com/pingcap/tidb/parser/mysql"
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
	ErrInternal                    = dbterror.ClassOptimizer.NewStd(mysql.ErrInternal)
	ErrNoDB                        = dbterror.ClassOptimizer.NewStd(mysql.ErrNoDB)

	// All the un-exported errors are defined here:
	errFunctionNotExists             = dbterror.ClassExpression.NewStd(mysql.ErrSpDoesNotExist)
	errZlibZData                     = dbterror.ClassExpression.NewStd(mysql.ErrZlibZData)
	errZlibZBuf                      = dbterror.ClassExpression.NewStd(mysql.ErrZlibZBuf)
	errIncorrectArgs                 = dbterror.ClassExpression.NewStd(mysql.ErrWrongArguments)
	errUnknownCharacterSet           = dbterror.ClassExpression.NewStd(mysql.ErrUnknownCharacterSet)
	errDefaultValue                  = dbterror.ClassExpression.NewStdErr(mysql.ErrInvalidDefault, pmysql.Message("invalid default value", nil))
	errDeprecatedSyntaxNoReplacement = dbterror.ClassExpression.NewStd(mysql.ErrWarnDeprecatedSyntaxNoReplacement)
	errWarnAllowedPacketOverflowed   = dbterror.ClassExpression.NewStd(mysql.ErrWarnAllowedPacketOverflowed)
	errWarnOptionIgnored             = dbterror.ClassExpression.NewStd(mysql.WarnOptionIgnored)
	errTruncatedWrongValue           = dbterror.ClassExpression.NewStd(mysql.ErrTruncatedWrongValue)
	errUnknownLocale                 = dbterror.ClassExpression.NewStd(mysql.ErrUnknownLocale)
	errNonUniq                       = dbterror.ClassExpression.NewStd(mysql.ErrNonUniq)
	errWrongValueForType             = dbterror.ClassExpression.NewStd(mysql.ErrWrongValueForType)
	errUnknown                       = dbterror.ClassExpression.NewStd(mysql.ErrUnknown)
	errSpecificAccessDenied          = dbterror.ClassExpression.NewStd(mysql.ErrSpecificAccessDenied)
	errUserLockDeadlock              = dbterror.ClassExpression.NewStd(mysql.ErrUserLockDeadlock)
	errUserLockWrongName             = dbterror.ClassExpression.NewStd(mysql.ErrUserLockWrongName)

	// Sequence usage privilege check.
	errSequenceAccessDenied      = dbterror.ClassExpression.NewStd(mysql.ErrTableaccessDenied)
	errUnsupportedJSONComparison = dbterror.ClassExpression.NewStdErr(mysql.ErrNotSupportedYet,
		pmysql.Message("comparison of JSON in the LEAST and GREATEST operators", nil))
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

// handleAllowedPacketOverflowed reports error or warning depend on the context.
func handleAllowedPacketOverflowed(ctx sessionctx.Context, exprName string, maxAllowedPacketSize uint64) error {
	err := errWarnAllowedPacketOverflowed.GenWithStackByArgs(exprName, maxAllowedPacketSize)
	sc := ctx.GetSessionVars().StmtCtx

	// insert|update|delete ignore ...
	if sc.TruncateAsWarning {
		sc.AppendWarning(err)
		return nil
	}

	if ctx.GetSessionVars().StrictSQLMode && (sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt) {
		return err
	}
	sc.AppendWarning(err)
	return nil
}
