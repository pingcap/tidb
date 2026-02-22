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
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type builtinRealAnyValueSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
