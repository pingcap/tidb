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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/vitess"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &sleepFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &anyValueFunctionClass{}
	_ functionClass = &defaultFunctionClass{}
	_ functionClass = &inetAtonFunctionClass{}
	_ functionClass = &inetNtoaFunctionClass{}
	_ functionClass = &inet6AtonFunctionClass{}
	_ functionClass = &inet6NtoaFunctionClass{}
	_ functionClass = &isFreeLockFunctionClass{}
	_ functionClass = &isIPv4FunctionClass{}
	_ functionClass = &isIPv4CompatFunctionClass{}
	_ functionClass = &isIPv4MappedFunctionClass{}
	_ functionClass = &isIPv6FunctionClass{}
	_ functionClass = &isUsedLockFunctionClass{}
	_ functionClass = &masterPosWaitFunctionClass{}
	_ functionClass = &nameConstFunctionClass{}
	_ functionClass = &releaseAllLocksFunctionClass{}
	_ functionClass = &uuidFunctionClass{}
	_ functionClass = &uuidShortFunctionClass{}
	_ functionClass = &vitessHashFunctionClass{}
	_ functionClass = &uuidToBinFunctionClass{}
	_ functionClass = &binToUUIDFunctionClass{}
	_ functionClass = &isUUIDFunctionClass{}
	_ functionClass = &tidbShardFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinReleaseAllLocksSig{}
	_ builtinFunc = &builtinDecimalAnyValueSig{}
	_ builtinFunc = &builtinDurationAnyValueSig{}
	_ builtinFunc = &builtinIntAnyValueSig{}
	_ builtinFunc = &builtinJSONAnyValueSig{}
	_ builtinFunc = &builtinRealAnyValueSig{}
	_ builtinFunc = &builtinStringAnyValueSig{}
	_ builtinFunc = &builtinTimeAnyValueSig{}
	_ builtinFunc = &builtinInetAtonSig{}
	_ builtinFunc = &builtinInetNtoaSig{}
	_ builtinFunc = &builtinInet6AtonSig{}
	_ builtinFunc = &builtinInet6NtoaSig{}
	_ builtinFunc = &builtinIsIPv4Sig{}
	_ builtinFunc = &builtinIsIPv4CompatSig{}
	_ builtinFunc = &builtinIsIPv4MappedSig{}
	_ builtinFunc = &builtinIsIPv6Sig{}
	_ builtinFunc = &builtinIsUUIDSig{}
	_ builtinFunc = &builtinUUIDSig{}
	_ builtinFunc = &builtinVitessHashSig{}
	_ builtinFunc = &builtinUUIDToBinSig{}
	_ builtinFunc = &builtinBinToUUIDSig{}

	_ builtinFunc = &builtinNameConstIntSig{}
	_ builtinFunc = &builtinNameConstRealSig{}
	_ builtinFunc = &builtinNameConstDecimalSig{}
	_ builtinFunc = &builtinNameConstTimeSig{}
	_ builtinFunc = &builtinNameConstDurationSig{}
	_ builtinFunc = &builtinNameConstStringSig{}
	_ builtinFunc = &builtinNameConstJSONSig{}
	_ builtinFunc = &builtinTidbShardSig{}
)

const (
	tidbShardBucketCount = 256
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
	contextopt.SessionVarsPropReader
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
	contextopt.AdvisoryLockPropReader
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
	maxTimeout := int64(variable.GetSysVar(variable.InnodbLockWaitTimeout).MaxValue)
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
	contextopt.AdvisoryLockPropReader
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
		return nil, errIncorrectArgs.GenWithStackByArgs("ANY_VALUE")
	}
	return sig, nil
}

type builtinDecimalAnyValueSig struct {
	baseBuiltinFunc
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

type builtinRealAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinRealAnyValueSig) Clone() builtinFunc {
	newSig := &builtinRealAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinRealAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinRealAnyValueSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	return b.args[0].EvalReal(ctx, row)
}

type builtinStringAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinStringAnyValueSig) Clone() builtinFunc {
	newSig := &builtinStringAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinStringAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinStringAnyValueSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return b.args[0].EvalString(ctx, row)
}

type builtinTimeAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeAnyValueSig) Clone() builtinFunc {
	newSig := &builtinTimeAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimeAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinTimeAnyValueSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	return b.args[0].EvalTime(ctx, row)
}

type defaultFunctionClass struct {
	baseFunctionClass
}

func (c *defaultFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	return nil, ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", "DEFAULT")
}

type inetAtonFunctionClass struct {
	baseFunctionClass
}

func (c *inetAtonFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(21)
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinInetAtonSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_InetAton)
	return sig, nil
}

type builtinInetAtonSig struct {
	baseBuiltinFunc
}

func (b *builtinInetAtonSig) Clone() builtinFunc {
	newSig := &builtinInetAtonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinInetAtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-aton
func (b *builtinInetAtonSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, true, err
	}
	// ip address should not end with '.'.
	if len(val) == 0 || val[len(val)-1] == '.' {
		return 0, false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton")
	}

	var (
		byteResult, result uint64
		dotCount           int
	)
	for _, c := range val {
		if c >= '0' && c <= '9' {
			digit := uint64(c - '0')
			byteResult = byteResult*10 + digit
			if byteResult > 255 {
				return 0, false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton")
			}
		} else if c == '.' {
			dotCount++
			if dotCount > 3 {
				return 0, false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton")
			}
			result = (result << 8) + byteResult
			byteResult = 0
		} else {
			return 0, false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton")
		}
	}
	// 127 		-> 0.0.0.127
	// 127.255 	-> 127.0.0.255
	// 127.256	-> NULL
	// 127.2.1	-> 127.2.0.1
	switch dotCount {
	case 1:
		result <<= 8
		fallthrough
	case 2:
		result <<= 8
	}
	return int64((result << 8) + byteResult), false, nil
}

type inetNtoaFunctionClass struct {
	baseFunctionClass
}

func (c *inetNtoaFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(93)
	bf.tp.SetDecimal(0)
	sig := &builtinInetNtoaSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_InetNtoa)
	return sig, nil
}

type builtinInetNtoaSig struct {
	baseBuiltinFunc
}

func (b *builtinInetNtoaSig) Clone() builtinFunc {
	newSig := &builtinInetNtoaSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInetNtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-ntoa
func (b *builtinInetNtoaSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil || isNull {
		return "", true, err
	}

	if val < 0 || uint64(val) > math.MaxUint32 {
		// not an IPv4 address.
		return "", true, nil
	}
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, uint32(val))
	ipv4 := ip.To4()
	if ipv4 == nil {
		// Not a valid ipv4 address.
		return "", true, nil
	}

	return ipv4.String(), false, nil
}

type inet6AtonFunctionClass struct {
	baseFunctionClass
}

func (c *inet6AtonFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(16)
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetDecimal(0)
	sig := &builtinInet6AtonSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Inet6Aton)
	return sig, nil
}

type builtinInet6AtonSig struct {
	baseBuiltinFunc
}

func (b *builtinInet6AtonSig) Clone() builtinFunc {
	newSig := &builtinInet6AtonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6AtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-aton
func (b *builtinInet6AtonSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return "", true, err
	}

	if len(val) == 0 {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton6")
	}

	ip := net.ParseIP(val)
	if ip == nil {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "inet_aton6")
	}

	var isMappedIpv6 bool
	if ip.To4() != nil && strings.Contains(val, ":") {
		// mapped ipv6 address.
		isMappedIpv6 = true
	}

	var result []byte
	if isMappedIpv6 || ip.To4() == nil {
		result = make([]byte, net.IPv6len)
	} else {
		result = make([]byte, net.IPv4len)
	}

	if isMappedIpv6 {
		copy(result[12:], ip.To4())
		result[11] = 0xff
		result[10] = 0xff
	} else if ip.To4() == nil {
		copy(result, ip.To16())
	} else {
		copy(result, ip.To4())
	}

	return string(result), false, nil
}

type inet6NtoaFunctionClass struct {
	baseFunctionClass
}

func (c *inet6NtoaFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(117)
	bf.tp.SetDecimal(0)
	sig := &builtinInet6NtoaSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Inet6Ntoa)
	return sig, nil
}

type builtinInet6NtoaSig struct {
	baseBuiltinFunc
}

func (b *builtinInet6NtoaSig) Clone() builtinFunc {
	newSig := &builtinInet6NtoaSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6NtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-ntoa
func (b *builtinInet6NtoaSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return "", true, err
	}
	ip := net.IP(val).String()
	if len(val) == net.IPv6len && !strings.Contains(ip, ":") {
		ip = fmt.Sprintf("::ffff:%s", ip)
	}

	if net.ParseIP(ip) == nil {
		return "", true, nil
	}

	return ip, false, nil
}

type isFreeLockFunctionClass struct {
	baseFunctionClass
}

func (c *isFreeLockFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinFreeLockSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(1)
	return sig, nil
}

type builtinFreeLockSig struct {
	baseBuiltinFunc
	contextopt.AdvisoryLockPropReader
}

func (b *builtinFreeLockSig) Clone() builtinFunc {
	newSig := &builtinFreeLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFreeLockSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.AdvisoryLockPropReader.RequiredOptionalEvalProps()
}

// See https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_is-free-lock
func (b *builtinFreeLockSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	lockCtx, err := b.AdvisoryLockCtx(ctx)
	if err != nil {
		return 0, true, err
	}

	lockName, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return 0, true, err
	}
	// Validate that lockName is NOT NULL or empty string
	if isNull {
		return 0, true, errUserLockWrongName.GenWithStackByArgs("NULL")
	}
	if lockName == "" || utf8.RuneCountInString(lockName) > 64 {
		return 0, true, errUserLockWrongName.GenWithStackByArgs(lockName)
	}

	// Lock names are case insensitive. Because we can't rely on collations
	// being enabled on the internal table, we have to lower it.
	lockName = strings.ToLower(lockName)
	if utf8.RuneCountInString(lockName) > 64 {
		return 0, true, errIncorrectArgs.GenWithStackByArgs("is_free_lock")
	}
	lock := lockCtx.IsUsedAdvisoryLock(lockName)
	if lock > 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type isIPv4FunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIsIPv4Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IsIPv4)
	return sig, nil
}

type builtinIsIPv4Sig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4Sig) Clone() builtinFunc {
	newSig := &builtinIsIPv4Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv4Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4
func (b *builtinIsIPv4Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}
	if isIPv4(val) {
		return 1, false, nil
	}
	return 0, false, nil
}

// isIPv4 checks IPv4 address which satisfying the format A.B.C.D(0<=A/B/C/D<=255).
// Mapped IPv6 address like '::ffff:1.2.3.4' would return false.
func isIPv4(ip string) bool {
	// acc: keep the decimal value of each segment under check, which should between 0 and 255 for valid IPv4 address.
	// pd: sentinel for '.'
	dots, acc, pd := 0, 0, true
	for _, c := range ip {
		switch {
		case '0' <= c && c <= '9':
			acc = acc*10 + int(c-'0')
			pd = false
		case c == '.':
			dots++
			if dots > 3 || acc > 255 || pd {
				return false
			}
			acc, pd = 0, true
		default:
			return false
		}
	}
	if dots != 3 || acc > 255 || pd {
		return false
	}
	return true
}

type isIPv4CompatFunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4CompatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIsIPv4CompatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IsIPv4Compat)
	return sig, nil
}

type builtinIsIPv4CompatSig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4CompatSig) Clone() builtinFunc {
	newSig := &builtinIsIPv4CompatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Compat
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
func (b *builtinIsIPv4CompatSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		// Not an IPv6 address, return false
		return 0, false, nil
	}

	prefixCompat := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.HasPrefix(ipAddress, prefixCompat) {
		return 0, false, nil
	}
	return 1, false, nil
}

type isIPv4MappedFunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4MappedFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIsIPv4MappedSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IsIPv4Mapped)
	return sig, nil
}

type builtinIsIPv4MappedSig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4MappedSig) Clone() builtinFunc {
	newSig := &builtinIsIPv4MappedSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Mapped
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
func (b *builtinIsIPv4MappedSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		// Not an IPv6 address, return false
		return 0, false, nil
	}

	prefixMapped := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}
	if !bytes.HasPrefix(ipAddress, prefixMapped) {
		return 0, false, nil
	}
	return 1, false, nil
}

type isIPv6FunctionClass struct {
	baseFunctionClass
}

func (c *isIPv6FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIsIPv6Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IsIPv6)
	return sig, nil
}

type builtinIsIPv6Sig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv6Sig) Clone() builtinFunc {
	newSig := &builtinIsIPv6Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv6Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv6
func (b *builtinIsIPv6Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}
	ip := net.ParseIP(val)
	if ip != nil && !isIPv4(val) {
		return 1, false, nil
	}
	return 0, false, nil
}

type isUsedLockFunctionClass struct {
	baseFunctionClass
}

func (c *isUsedLockFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinUsedLockSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(1)
	return sig, nil
}

type builtinUsedLockSig struct {
	baseBuiltinFunc
	contextopt.AdvisoryLockPropReader
}

func (b *builtinUsedLockSig) Clone() builtinFunc {
	newSig := &builtinUsedLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUsedLockSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.AdvisoryLockPropReader.RequiredOptionalEvalProps()
}

// See https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_is-used-lock
func (b *builtinUsedLockSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
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
		return 0, false, errIncorrectArgs.GenWithStackByArgs("is_used_lock")
	}
	lock := lockCtx.IsUsedAdvisoryLock(lockName)
	return int64(lock), lock == 0, nil // TODO, uint64
}

type isUUIDFunctionClass struct {
	baseFunctionClass
}

func (c *isUUIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIsUUIDSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IsUUID)
	return sig, nil
}

type builtinIsUUIDSig struct {
	baseBuiltinFunc
}

func (b *builtinIsUUIDSig) Clone() builtinFunc {
	newSig := &builtinIsUUIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsUUIDSig.
// See https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_is-uuid
func (b *builtinIsUUIDSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}
	if _, err = uuid.Parse(val); err != nil {
		return 0, false, nil
	}
	return 1, false, nil
}

type masterPosWaitFunctionClass struct {
	baseFunctionClass
}

func (c *masterPosWaitFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	return nil, ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", "MASTER_POS_WAIT")
}

type nameConstFunctionClass struct {
	baseFunctionClass
}

func (c *nameConstFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETString, argTp)
	if err != nil {
		return nil, err
	}
	*bf.tp = *args[1].GetType(ctx.GetEvalCtx())
	var sig builtinFunc
	switch argTp {
	case types.ETDecimal:
		sig = &builtinNameConstDecimalSig{bf}
	case types.ETDuration:
		sig = &builtinNameConstDurationSig{bf}
	case types.ETInt:
		bf.tp.SetDecimal(0)
		sig = &builtinNameConstIntSig{bf}
	case types.ETJson:
		sig = &builtinNameConstJSONSig{bf}
	case types.ETReal:
		sig = &builtinNameConstRealSig{bf}
	case types.ETString:
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinNameConstStringSig{bf}
	case types.ETDatetime, types.ETTimestamp:
		bf.tp.SetCharset(mysql.DefaultCharset)
		bf.tp.SetCollate(mysql.DefaultCollationName)
		bf.tp.SetFlag(0)
		sig = &builtinNameConstTimeSig{bf}
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("NAME_CONST")
	}
	return sig, nil
}

type builtinNameConstDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstDecimalSig) Clone() builtinFunc {
	newSig := &builtinNameConstDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	return b.args[1].EvalDecimal(ctx, row)
}

type builtinNameConstIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstIntSig) Clone() builtinFunc {
	newSig := &builtinNameConstIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[1].EvalInt(ctx, row)
}

type builtinNameConstRealSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstRealSig) Clone() builtinFunc {
	newSig := &builtinNameConstRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	return b.args[1].EvalReal(ctx, row)
}

type builtinNameConstStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstStringSig) Clone() builtinFunc {
	newSig := &builtinNameConstStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return b.args[1].EvalString(ctx, row)
}

type builtinNameConstJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstJSONSig) Clone() builtinFunc {
	newSig := &builtinNameConstJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	return b.args[1].EvalJSON(ctx, row)
}

type builtinNameConstDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstDurationSig) Clone() builtinFunc {
	newSig := &builtinNameConstDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	return b.args[1].EvalDuration(ctx, row)
}

type builtinNameConstTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstTimeSig) Clone() builtinFunc {
	newSig := &builtinNameConstTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	return b.args[1].EvalTime(ctx, row)
}

type releaseAllLocksFunctionClass struct {
	baseFunctionClass
}

func (c *releaseAllLocksFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinReleaseAllLocksSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(1)
	return sig, nil
}

type builtinReleaseAllLocksSig struct {
	baseBuiltinFunc
	contextopt.AdvisoryLockPropReader
}

func (b *builtinReleaseAllLocksSig) Clone() builtinFunc {
	newSig := &builtinReleaseAllLocksSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinReleaseAllLocksSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.AdvisoryLockPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinReleaseAllLocksSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_release-all-locks
func (b *builtinReleaseAllLocksSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	lockCtx, err := b.AdvisoryLockCtx(ctx)
	if err != nil {
		return 0, false, err
	}
	count := lockCtx.ReleaseAllAdvisoryLocks()
	return int64(count), false, nil
}

type uuidFunctionClass struct {
	baseFunctionClass
}

func (c *uuidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(36)
	sig := &builtinUUIDSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UUID)
	return sig, nil
}

type builtinUUIDSig struct {
	baseBuiltinFunc
}

func (b *builtinUUIDSig) Clone() builtinFunc {
	newSig := &builtinUUIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_uuid
func (b *builtinUUIDSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var id uuid.UUID
	id, err = uuid.NewUUID()
	if err != nil {
		return
	}
	d = id.String()
	return
}

type uuidShortFunctionClass struct {
	baseFunctionClass
}

func (c *uuidShortFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	return nil, ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", "UUID_SHORT")
}

type vitessHashFunctionClass struct {
	baseFunctionClass
}

func (c *vitessHashFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(20) //64 bit unsigned
	bf.tp.AddFlag(mysql.UnsignedFlag)
	types.SetBinChsClnFlag(bf.tp)

	sig := &builtinVitessHashSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VitessHash)
	return sig, nil
}

type builtinVitessHashSig struct {
	baseBuiltinFunc
}

func (b *builtinVitessHashSig) Clone() builtinFunc {
	newSig := &builtinVitessHashSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals VITESS_HASH(int64).
func (b *builtinVitessHashSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	shardKeyInt, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	var hashed uint64
	if hashed, err = vitess.HashUint64(uint64(shardKeyInt)); err != nil {
		return 0, true, err
	}
	return int64(hashed), false, nil
}

type uuidToBinFunctionClass struct {
	baseFunctionClass
}

func (c *uuidToBinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(16)
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.SetDecimal(0)
	sig := &builtinUUIDToBinSig{bf}
	return sig, nil
}

type builtinUUIDToBinSig struct {
	baseBuiltinFunc
}

func (b *builtinUUIDToBinSig) Clone() builtinFunc {
	newSig := &builtinUUIDToBinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals UUID_TO_BIN(string_uuid, swap_flag).
// See https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_uuid-to-bin
func (b *builtinUUIDToBinSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	u, err := uuid.Parse(val)
	if err != nil {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
	}
	bin, err := u.MarshalBinary()
	if err != nil {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
	}

	flag := int64(0)
	if len(b.args) == 2 {
		flag, isNull, err = b.args[1].EvalInt(ctx, row)
		if isNull {
			flag = 0
		}
		if err != nil {
			return "", false, err
		}
	}
	if flag != 0 {
		return swapBinaryUUID(bin), false, nil
	}
	return string(bin), false, nil
}

type binToUUIDFunctionClass struct {
	baseFunctionClass
}

func (c *binToUUIDFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}

	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(32)
	bf.tp.SetDecimal(0)
	sig := &builtinBinToUUIDSig{bf}
	return sig, nil
}

type builtinBinToUUIDSig struct {
	baseBuiltinFunc
}

func (b *builtinBinToUUIDSig) Clone() builtinFunc {
	newSig := &builtinBinToUUIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals BIN_TO_UUID(binary_uuid, swap_flag).
// See https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_bin-to-uuid
func (b *builtinBinToUUIDSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	var u uuid.UUID
	err = u.UnmarshalBinary([]byte(val))
	if err != nil {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "bin_to_uuid")
	}

	str := u.String()
	flag := int64(0)
	if len(b.args) == 2 {
		flag, isNull, err = b.args[1].EvalInt(ctx, row)
		if isNull {
			flag = 0
		}
		if err != nil {
			return "", false, err
		}
	}
	if flag != 0 {
		return swapStringUUID(str), false, nil
	}
	return str, false, nil
}

func swapBinaryUUID(bin []byte) string {
	buf := make([]byte, len(bin))
	copy(buf[0:2], bin[6:8])
	copy(buf[2:4], bin[4:6])
	copy(buf[4:8], bin[0:4])
	copy(buf[8:], bin[8:])
	return string(buf)
}

func swapStringUUID(str string) string {
	buf := make([]byte, len(str))
	copy(buf[0:4], str[9:13])
	copy(buf[4:8], str[14:18])
	copy(buf[8:9], str[8:9])
	copy(buf[9:13], str[4:8])
	copy(buf[13:14], str[13:14])
	copy(buf[14:18], str[0:4])
	copy(buf[18:], str[18:])
	return string(buf)
}

type tidbShardFunctionClass struct {
	baseFunctionClass
}

func (c *tidbShardFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(4) //64 bit unsigned
	bf.tp.AddFlag(mysql.UnsignedFlag)
	types.SetBinChsClnFlag(bf.tp)

	sig := &builtinTidbShardSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TiDBShard)
	return sig, nil
}

type builtinTidbShardSig struct {
	baseBuiltinFunc
}

func (b *builtinTidbShardSig) Clone() builtinFunc {
	newSig := &builtinTidbShardSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals tidb_shard(int64).
func (b *builtinTidbShardSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	shardKeyInt, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	var hashed uint64
	if hashed, err = vitess.HashUint64(uint64(shardKeyInt)); err != nil {
		return 0, true, err
	}
	hashed = hashed % tidbShardBucketCount
	return int64(hashed), false, nil
}

type tidbRowChecksumFunctionClass struct {
	baseFunctionClass
}

func (c *tidbRowChecksumFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	return nil, ErrNotSupportedYet.GenWithStack("FUNCTION tidb_row_checksum can only be used as a select field in a fast point plan")
}
