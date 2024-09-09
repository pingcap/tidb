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
	"github.com/pingcap/errors"
	mysql "github.com/pingcap/tidb/pkg/errno"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
	ErrInvalidTypeForJSON          = dbterror.ClassExpression.NewStd(mysql.ErrInvalidTypeForJSON)
	ErrInvalidTableSample          = dbterror.ClassExpression.NewStd(mysql.ErrInvalidTableSample)
	ErrNotSupportedYet             = dbterror.ClassExpression.NewStd(mysql.ErrNotSupportedYet)
	ErrInvalidJSONForFuncIndex     = dbterror.ClassExpression.NewStd(mysql.ErrInvalidJSONValueForFuncIndex)
	ErrDataOutOfRangeFuncIndex     = dbterror.ClassExpression.NewStd(mysql.ErrDataOutOfRangeFunctionalIndex)
	ErrFuncIndexDataIsTooLong      = dbterror.ClassExpression.NewStd(mysql.ErrFunctionalIndexDataIsTooLong)
	ErrFunctionNotExists           = dbterror.ClassExpression.NewStd(mysql.ErrSpDoesNotExist)

	// All the un-exported errors are defined here:
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
	errJSONInBooleanContext          = dbterror.ClassExpression.NewStd(mysql.ErrJSONInBooleanContext)
	errBadNull                       = dbterror.ClassExpression.NewStd(mysql.ErrBadNull)

	// Sequence usage privilege check.
	errSequenceAccessDenied      = dbterror.ClassExpression.NewStd(mysql.ErrTableaccessDenied)
	errUnsupportedJSONComparison = dbterror.ClassExpression.NewStdErr(mysql.ErrNotSupportedYet,
		pmysql.Message("comparison of JSON in the LEAST and GREATEST operators", nil))
)

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx EvalContext, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) || types.ErrWrongValueForType.Equal(err) ||
		types.ErrTruncatedWrongVal.Equal(err) || types.ErrInvalidWeekModeFormat.Equal(err) ||
		types.ErrDatetimeFunctionOverflow.Equal(err) || types.ErrIncorrectDatetimeValue.Equal(err)) {
		return err
	}
	ec := errCtx(ctx)
	return ec.HandleError(err)
}

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx EvalContext) error {
	ec := errCtx(ctx)
	return ec.HandleError(ErrDivisionByZero)
}

// handleAllowedPacketOverflowed reports error or warning depend on the context.
func handleAllowedPacketOverflowed(ctx EvalContext, exprName string, maxAllowedPacketSize uint64) error {
	err := errWarnAllowedPacketOverflowed.FastGenByArgs(exprName, maxAllowedPacketSize)
	tc := typeCtx(ctx)
	if f := tc.Flags(); f.TruncateAsWarning() || f.IgnoreTruncateErr() {
		tc.AppendWarning(err)
		return nil
	}
	return errors.Trace(err)
}
