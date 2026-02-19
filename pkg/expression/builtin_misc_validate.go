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
	"net"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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
	expropt.AdvisoryLockPropReader
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, true, nil
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, true, nil
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, true, nil
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, true, nil
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
	expropt.AdvisoryLockPropReader
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// MySQL's IS_UUID is strict and doesn't trim spaces, unlike Go's uuid.Parse
	// We need to check if the string has leading/trailing spaces before parsing
	if strings.TrimSpace(val) != val {
		return 0, false, nil
	}
	if _, err = uuid.Parse(val); err != nil {
		return 0, false, nil
	}
	return 1, false, nil
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
	case types.ETVectorFloat32:
		sig = &builtinNameConstVectorFloat32Sig{bf}
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
		return nil, errors.Errorf("%s is not supported for NAME_CONST()", argTp)
	}
	return sig, nil
}

type builtinNameConstDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNameConstJSONSig) Clone() builtinFunc {
	newSig := &builtinNameConstJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	return b.args[1].EvalJSON(ctx, row)
}

type builtinNameConstVectorFloat32Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNameConstVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinNameConstVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	return b.args[1].EvalVectorFloat32(ctx, row)
}

type builtinNameConstDurationSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	expropt.AdvisoryLockPropReader
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
