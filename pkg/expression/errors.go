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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount     = dbterror.ClassExpression.NewStd(errno.ErrWrongParamcountToNativeFct)
	ErrDivisionByZero              = dbterror.ClassExpression.NewStd(errno.ErrDivisionByZero)
	ErrRegexp                      = dbterror.ClassExpression.NewStd(errno.ErrRegexp)
	ErrOperandColumns              = dbterror.ClassExpression.NewStd(errno.ErrOperandColumns)
	ErrCutValueGroupConcat         = dbterror.ClassExpression.NewStd(errno.ErrCutValueGroupConcat)
	ErrFunctionsNoopImpl           = dbterror.ClassExpression.NewStdErr(errno.ErrNotSupportedYet, errno.Message("function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", nil))
	ErrInvalidArgumentForLogarithm = dbterror.ClassExpression.NewStd(errno.ErrInvalidArgumentForLogarithm)
	ErrIncorrectType               = dbterror.ClassExpression.NewStd(errno.ErrIncorrectType)
	ErrInvalidTypeForJSON          = dbterror.ClassExpression.NewStd(errno.ErrInvalidTypeForJSON)
	ErrInvalidTableSample          = dbterror.ClassExpression.NewStd(errno.ErrInvalidTableSample)
	ErrNotSupportedYet             = dbterror.ClassExpression.NewStd(errno.ErrNotSupportedYet)
	ErrInvalidJSONForFuncIndex     = dbterror.ClassExpression.NewStd(errno.ErrInvalidJSONValueForFuncIndex)
	ErrDataOutOfRangeFuncIndex     = dbterror.ClassExpression.NewStd(errno.ErrDataOutOfRangeFunctionalIndex)
	ErrFuncIndexDataIsTooLong      = dbterror.ClassExpression.NewStd(errno.ErrFunctionalIndexDataIsTooLong)
	ErrFunctionNotExists           = dbterror.ClassExpression.NewStd(errno.ErrSpDoesNotExist)

	// All the un-exported errors are defined here:
	errZlibZData                     = dbterror.ClassExpression.NewStd(errno.ErrZlibZData)
	errZlibZBuf                      = dbterror.ClassExpression.NewStd(errno.ErrZlibZBuf)
	errIncorrectArgs                 = dbterror.ClassExpression.NewStd(errno.ErrWrongArguments)
	errUnknownCharacterSet           = dbterror.ClassExpression.NewStd(errno.ErrUnknownCharacterSet)
	errDefaultValue                  = dbterror.ClassExpression.NewStdErr(errno.ErrInvalidDefault, errno.Message("invalid default value", nil))
	errDeprecatedSyntaxNoReplacement = dbterror.ClassExpression.NewStd(errno.ErrWarnDeprecatedSyntaxNoReplacement)
	errWarnAllowedPacketOverflowed   = dbterror.ClassExpression.NewStd(errno.ErrWarnAllowedPacketOverflowed)
	errWarnOptionIgnored             = dbterror.ClassExpression.NewStd(errno.WarnOptionIgnored)
	errTruncatedWrongValue           = dbterror.ClassExpression.NewStd(errno.ErrTruncatedWrongValue)
	errUnknownLocale                 = dbterror.ClassExpression.NewStd(errno.ErrUnknownLocale)
	errNonUniq                       = dbterror.ClassExpression.NewStd(errno.ErrNonUniq)
	errWrongValueForType             = dbterror.ClassExpression.NewStd(errno.ErrWrongValueForType)
	errUnknown                       = dbterror.ClassExpression.NewStd(errno.ErrUnknown)
	errSpecificAccessDenied          = dbterror.ClassExpression.NewStd(errno.ErrSpecificAccessDenied)
	errUserLockDeadlock              = dbterror.ClassExpression.NewStd(errno.ErrUserLockDeadlock)
	errUserLockWrongName             = dbterror.ClassExpression.NewStd(errno.ErrUserLockWrongName)
	errJSONInBooleanContext          = dbterror.ClassExpression.NewStd(errno.ErrJSONInBooleanContext)
	errBadNull                       = dbterror.ClassExpression.NewStd(errno.ErrBadNull)

	// Sequence usage privilege check.
	errSequenceAccessDenied      = dbterror.ClassExpression.NewStd(errno.ErrTableaccessDenied)
	errUnsupportedJSONComparison = dbterror.ClassExpression.NewStdErr(errno.ErrNotSupportedYet,
		errno.Message("comparison of JSON in the LEAST and GREATEST operators", nil))
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
