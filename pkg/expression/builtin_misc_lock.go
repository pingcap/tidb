// Copyright 2021 PingCAP, Inc.
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
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type sleepFunctionClass struct {
	baseFunctionClass
}

func (c *sleepFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(21)
	sig := &builtinSleepSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinSleepSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSleepSig) Clone() builtinFunc {
	newSig := &builtinSleepSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSleepSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinSleepSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}

	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, isNull, err
	}

	ec := errCtx(ctx)
	if isNull || val < 0 {
		// for insert ignore stmt, the StrictSQLMode and ignoreErr should both be considered.
		return 0, false, ec.HandleErrorWithAlias(
			errBadNull,
			errIncorrectArgs.GenWithStackByArgs("sleep"),
			errIncorrectArgs.FastGenByArgs("sleep"),
		)
	}

	if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("sleep")
	}

	if isKilled := doSleep(val, vars); isKilled {
		return 1, false, nil
	}

	return 0, false, nil
}

type lockFunctionClass struct {
	baseFunctionClass
}

func (c *lockFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinLockSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(1)
	return sig, nil
}

type builtinLockSig struct {
	baseBuiltinFunc
	expropt.AdvisoryLockPropReader
}

func (b *builtinLockSig) Clone() builtinFunc {
	newSig := &builtinLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLockSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.AdvisoryLockPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
func (b *builtinLockSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	lockCtx, err := b.AdvisoryLockCtx(ctx)
	if err != nil {
		return 0, false, err
	}

	lockName, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return 0, isNull, err
	}
	// Validate that lockName is NOT NULL or empty string
	if isNull {
		return 0, false, errUserLockWrongName.GenWithStackByArgs("NULL")
	}
	if lockName == "" || utf8.RuneCountInString(lockName) > 64 {
		return 0, false, errUserLockWrongName.GenWithStackByArgs(lockName)
	}
	maxTimeout := int64(variable.GetSysVar(vardef.InnodbLockWaitTimeout).MaxValue)
	timeout, isNullTimeout, err := b.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if isNullTimeout {
		timeout = 0 // Observed in MySQL, gets converted to 1s in TiDB because of min timeout.
	}
	// A timeout less than zero is expected to be treated as unlimited.
	// Because of our implementation being based on pessimistic locks,
	// We can't have a timeout greater than innodb_lock_wait_timeout.
	// So users are aware, we also attach a warning.
	if timeout < 0 || timeout > maxTimeout {
		err := errTruncatedWrongValue.FastGenByArgs("get_lock", strconv.FormatInt(timeout, 10))
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		timeout = maxTimeout
	}

	// Lock names are case insensitive. Because we can't rely on collations
	// being enabled on the internal table, we have to lower it.
	lockName = strings.ToLower(lockName)
	if utf8.RuneCountInString(lockName) > 64 {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("get_lock")
	}
	err = lockCtx.GetAdvisoryLock(lockName, timeout)
	if err != nil {
		if terr, ok := errors.Cause(err).(*terror.Error); ok {
			switch terr.Code() {
			case mysql.ErrLockWaitTimeout:
				return 0, false, nil // Another user has the lock
			case mysql.ErrLockDeadlock:
				// Currently this code is not reachable because each Advisory Lock
				// Uses a separate session. Deadlock detection does not work across
				// independent sessions.
				return 0, false, errUserLockDeadlock
			}
		}
		return 0, false, err
	}
	return 1, false, nil
}

type releaseLockFunctionClass struct {
	baseFunctionClass
}

func (c *releaseLockFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinReleaseLockSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(1)
	return sig, nil
}

type builtinReleaseLockSig struct {
	baseBuiltinFunc
	expropt.AdvisoryLockPropReader
}

func (b *builtinReleaseLockSig) Clone() builtinFunc {
	newSig := &builtinReleaseLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinReleaseLockSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.AdvisoryLockPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinReleaseLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_release-lock
func (b *builtinReleaseLockSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	lockCtx, err := b.AdvisoryLockCtx(ctx)
	if err != nil {
		return 0, false, err
	}

	lockName, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return 0, isNull, err
	}
	// Validate that lockName is NOT NULL or empty string
	if isNull {
		return 0, false, errUserLockWrongName.GenWithStackByArgs("NULL")
	}
	if lockName == "" || utf8.RuneCountInString(lockName) > 64 {
		return 0, false, errUserLockWrongName.GenWithStackByArgs(lockName)
	}
	// Lock names are case insensitive. Because we can't rely on collations
	// being enabled on the internal table, we have to lower it.
	lockName = strings.ToLower(lockName)
	if utf8.RuneCountInString(lockName) > 64 {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("release_lock")
	}
	released := int64(0)
	if lockCtx.ReleaseAdvisoryLock(lockName) {
		released = 1
	}
	return released, false, nil
}

type anyValueFunctionClass struct {
	baseFunctionClass
}

func (c *anyValueFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTp)
	if err != nil {
		return nil, err
	}
	ft := args[0].GetType(ctx.GetEvalCtx()).Clone()
	ft.AddFlag(bf.tp.GetFlag())
	*bf.tp = *ft
	var sig builtinFunc
	switch argTp {
	case types.ETDecimal:
		sig = &builtinDecimalAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DecimalAnyValue)
	case types.ETDuration:
		sig = &builtinDurationAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DurationAnyValue)
	case types.ETInt:
		bf.tp.SetDecimal(0)
		sig = &builtinIntAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntAnyValue)
	case types.ETJson:
		sig = &builtinJSONAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JSONAnyValue)
	case types.ETVectorFloat32:
		sig = &builtinVectorFloat32AnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_VectorFloat32AnyValue)
	case types.ETReal:
		sig = &builtinRealAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_RealAnyValue)
	case types.ETString:
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinStringAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StringAnyValue)
	case types.ETDatetime, types.ETTimestamp:
		bf.tp.SetCharset(mysql.DefaultCharset)
		bf.tp.SetCollate(mysql.DefaultCollationName)
		bf.tp.SetFlag(0)
		sig = &builtinTimeAnyValueSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_TimeAnyValue)
	default:
		return nil, errors.Errorf("%s is not supported for ANY_VALUE()", argTp)
	}
	return sig, nil
}

type builtinDecimalAnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDecimalAnyValueSig) Clone() builtinFunc {
	newSig := &builtinDecimalAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinDecimalAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDecimalAnyValueSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	return b.args[0].EvalDecimal(ctx, row)
}

type builtinDurationAnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDurationAnyValueSig) Clone() builtinFunc {
	newSig := &builtinDurationAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDurationAnyValueSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	return b.args[0].EvalDuration(ctx, row)
}

type builtinIntAnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIntAnyValueSig) Clone() builtinFunc {
	newSig := &builtinIntAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinIntAnyValueSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinJSONAnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONAnyValueSig) Clone() builtinFunc {
	newSig := &builtinJSONAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinJSONAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinJSONAnyValueSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	return b.args[0].EvalJSON(ctx, row)
}

type builtinVectorFloat32AnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinVectorFloat32AnyValueSig) Clone() builtinFunc {
	newSig := &builtinVectorFloat32AnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinVectorFloat32AnyValueSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	return b.args[0].EvalVectorFloat32(ctx, row)
}
