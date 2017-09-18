// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
	"github.com/twinj/uuid"
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
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
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
	_ builtinFunc = &builtinUUIDSig{}
)

type sleepFunctionClass struct {
	baseFunctionClass
}

func (c *sleepFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpReal)
	bf.tp.Flen = 21
	bf.foldable = false
	sig := &builtinSleepSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSleepSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinSleepSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, isNull, errors.Trace(err)
	}
	sessVars := b.ctx.GetSessionVars()
	if isNull {
		if sessVars.StrictSQLMode {
			return 0, true, errIncorrectArgs.GenByArgs("sleep")
		}
		return 0, true, nil
	}
	// processing argument is negative
	if val < 0 {
		if sessVars.StrictSQLMode {
			return 0, false, errIncorrectArgs.GenByArgs("sleep")
		}
		return 0, false, nil
	}

	// TODO: consider it's interrupted using KILL QUERY from other session, or
	// interrupted by time out.
	if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
		return 0, false, errIncorrectArgs.GenByArgs("sleep")
	}
	dur := time.Duration(val * float64(time.Second.Nanoseconds()))
	time.Sleep(dur)
	return 0, false, nil
}

type lockFunctionClass struct {
	baseFunctionClass
}

func (c *lockFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpInt)
	sig := &builtinLockSig{baseIntBuiltinFunc{bf}}
	bf.tp.Flen = 1
	return sig.setSelf(sig), nil
}

type builtinLockSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
// The lock function will do nothing.
// Warning: get_lock() function is parsed but ignored.
func (b *builtinLockSig) evalInt(_ []types.Datum) (int64, bool, error) {
	return 1, false, nil
}

type releaseLockFunctionClass struct {
	baseFunctionClass
}

func (c *releaseLockFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	sig := &builtinReleaseLockSig{baseIntBuiltinFunc{bf}}
	bf.tp.Flen = 1
	return sig.setSelf(sig), nil
}

type builtinReleaseLockSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinReleaseLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_release-lock
// The release lock function will do nothing.
// Warning: release_lock() function is parsed but ignored.
func (b *builtinReleaseLockSig) evalInt(_ []types.Datum) (int64, bool, error) {
	return 1, false, nil
}

type anyValueFunctionClass struct {
	baseFunctionClass
}

func (c *anyValueFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTp := fieldTp2EvalTp(args[0].GetType())
	bf := newBaseBuiltinFuncWithTp(args, ctx, argTp, argTp)
	*bf.tp = *args[0].GetType()
	var sig builtinFunc
	switch argTp {
	case tpDecimal:
		sig = &builtinDecimalAnyValueSig{baseDecimalBuiltinFunc{bf}}
	case tpDuration:
		sig = &builtinDurationAnyValueSig{baseDurationBuiltinFunc{bf}}
	case tpInt:
		bf.tp.Decimal = 0
		sig = &builtinIntAnyValueSig{baseIntBuiltinFunc{bf}}
	case tpJSON:
		sig = &builtinJSONAnyValueSig{baseJSONBuiltinFunc{bf}}
	case tpReal:
		sig = &builtinRealAnyValueSig{baseRealBuiltinFunc{bf}}
	case tpString:
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinStringAnyValueSig{baseStringBuiltinFunc{bf}}
	case tpDatetime, tpTimestamp:
		bf.tp.Charset, bf.tp.Collate, bf.tp.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
		sig = &builtinTimeAnyValueSig{baseTimeBuiltinFunc{bf}}
	default:
		panic("unexpected evalTp of builtin function ANY_VALUE")
	}
	return sig.setSelf(sig), nil
}

type builtinDecimalAnyValueSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals a builtinDecimalAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDecimalAnyValueSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	return b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinDurationAnyValueSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinDurationAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDurationAnyValueSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	return b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinIntAnyValueSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinIntAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinIntAnyValueSig) evalInt(row []types.Datum) (int64, bool, error) {
	return b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinJSONAnyValueSig struct {
	baseJSONBuiltinFunc
}

// evalJSON evals a builtinJSONAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinJSONAnyValueSig) evalJSON(row []types.Datum) (json.JSON, bool, error) {
	return b.args[0].EvalJSON(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinRealAnyValueSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinRealAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinRealAnyValueSig) evalReal(row []types.Datum) (float64, bool, error) {
	return b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinStringAnyValueSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinStringAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinStringAnyValueSig) evalString(row []types.Datum) (string, bool, error) {
	return b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinTimeAnyValueSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinTimeAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinTimeAnyValueSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	return b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
}

type defaultFunctionClass struct {
	baseFunctionClass
}

func (c *defaultFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("DEFAULT")
}

type inetAtonFunctionClass struct {
	baseFunctionClass
}

func (c *inetAtonFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 21
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinInetAtonSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinInetAtonSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinInetAtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-aton
func (b *builtinInetAtonSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return 0, true, errors.Trace(err)
	}
	// ip address should not end with '.'.
	if len(val) == 0 || val[len(val)-1] == '.' {
		return 0, true, nil
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
				return 0, true, nil
			}
		} else if c == '.' {
			dotCount++
			if dotCount > 3 {
				return 0, true, nil
			}
			result = (result << 8) + byteResult
			byteResult = 0
		} else {
			return 0, true, nil
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

func (c *inetNtoaFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
	bf.tp.Flen = 93
	bf.tp.Decimal = 0
	sig := &builtinInetNtoaSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinInetNtoaSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinInetNtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-ntoa
func (b *builtinInetNtoaSig) evalString(row []types.Datum) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return "", true, errors.Trace(err)
	}

	if val < 0 || uint64(val) > math.MaxUint32 {
		//not an IPv4 address.
		return "", true, nil
	}
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, uint32(val))
	ipv4 := ip.To4()
	if ipv4 == nil {
		//Not a vaild ipv4 address.
		return "", true, nil
	}

	return ipv4.String(), false, nil
}

type inet6AtonFunctionClass struct {
	baseFunctionClass
}

func (c *inet6AtonFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = 16
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.Decimal = 0
	sig := &builtinInet6AtonSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinInet6AtonSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinInet6AtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-aton
func (b *builtinInet6AtonSig) evalString(row []types.Datum) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return "", true, errors.Trace(err)
	}

	if len(val) == 0 {
		return "", true, nil
	}

	ip := net.ParseIP(val)
	if ip == nil {
		return "", true, nil
	}

	var isMappedIpv6 bool
	if ip.To4() != nil && strings.Contains(val, ":") {
		//mapped ipv6 address.
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

	return string(result[:]), false, nil
}

type inet6NtoaFunctionClass struct {
	baseFunctionClass
}

func (c *inet6NtoaFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = 117
	bf.tp.Decimal = 0
	sig := &builtinInet6NtoaSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinInet6NtoaSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinInet6NtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-ntoa
func (b *builtinInet6NtoaSig) evalString(row []types.Datum) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return "", true, errors.Trace(err)
	}
	ip := net.IP([]byte(val)).String()
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

func (c *isFreeLockFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("IS_FREE_LOCK")
}

type isIPv4FunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4Sig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinIsIPv4Sig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinIsIPv4Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4
func (b *builtinIsIPv4Sig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return 0, false, errors.Trace(err)
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

func (c *isIPv4CompatFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4CompatSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinIsIPv4CompatSig struct {
	baseIntBuiltinFunc
}

// evalInt evals Is_IPv4_Compat
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
func (b *builtinIsIPv4CompatSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return 0, false, errors.Trace(err)
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		//Not an IPv6 address, return false
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

func (c *isIPv4MappedFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4MappedSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinIsIPv4MappedSig struct {
	baseIntBuiltinFunc
}

// evalInt evals Is_IPv4_Mapped
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
func (b *builtinIsIPv4MappedSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return int64(0), false, errors.Trace(err)
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		//Not an IPv6 address, return false
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

func (c *isIPv6FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv6Sig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinIsIPv6Sig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinIsIPv6Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv6
func (b *builtinIsIPv6Sig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return 0, false, errors.Trace(err)
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

func (c *isUsedLockFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("IS_USED_LOCK")
}

type masterPosWaitFunctionClass struct {
	baseFunctionClass
}

func (c *masterPosWaitFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("MASTER_POS_WAIT")
}

type nameConstFunctionClass struct {
	baseFunctionClass
}

func (c *nameConstFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("NAME_CONST")
}

type releaseAllLocksFunctionClass struct {
	baseFunctionClass
}

func (c *releaseAllLocksFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("RELEASE_ALL_LOCKS")
}

type uuidFunctionClass struct {
	baseFunctionClass
}

func (c *uuidFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString)
	bf.tp.Flen = 36
	bf.foldable = false
	sig := &builtinUUIDSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUUIDSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinUUIDSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_uuid
func (b *builtinUUIDSig) evalString(_ []types.Datum) (d string, isNull bool, err error) {
	return uuid.NewV1().String(), false, nil
}

type uuidShortFunctionClass struct {
	baseFunctionClass
}

func (c *uuidShortFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("UUID_SHORT")
}
