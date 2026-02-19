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
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/vitess"
	"github.com/pingcap/tipb/go-tipb"
)

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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUUIDSig) Clone() builtinFunc {
	newSig := &builtinUUIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDSig.
// See:
// - https://dev.mysql.com/doc/refman/8.4/en/miscellaneous-functions.html#function_uuid
// - https://datatracker.ietf.org/doc/html/rfc9562
func (b *builtinUUIDSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var id uuid.UUID
	id, err = uuid.NewUUID()
	if err != nil {
		return
	}
	d = id.String()
	return
}

// Function: UUID_V4()
type uuidv4FunctionClass struct {
	baseFunctionClass
}

func (c *uuidv4FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinUUIDv4Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UUIDv4)
	return sig, nil
}

type builtinUUIDv4Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUUIDv4Sig) Clone() builtinFunc {
	newSig := &builtinUUIDv4Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDv4Sig.
func (b *builtinUUIDv4Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var id uuid.UUID
	id, err = uuid.NewRandom()
	if err != nil {
		return
	}
	d = id.String()
	return
}

// Function: UUID_V7()
type uuidv7FunctionClass struct {
	baseFunctionClass
}

func (c *uuidv7FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinUUIDv7Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UUIDv7)
	return sig, nil
}

type builtinUUIDv7Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUUIDv7Sig) Clone() builtinFunc {
	newSig := &builtinUUIDv7Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDv7Sig.
func (b *builtinUUIDv7Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var id uuid.UUID
	id, err = uuid.NewV7()
	if err != nil {
		return
	}
	d = id.String()
	return
}

// Function: UUID_VERSION()
type uuidVersionFunctionClass struct {
	baseFunctionClass
}

func (c *uuidVersionFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinUUIDVersionSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UUIDVersion)
	return sig, nil
}

type builtinUUIDVersionSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUUIDVersionSig) Clone() builtinFunc {
	newSig := &builtinUUIDVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinUUIDVersionSig.
func (b *builtinUUIDVersionSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	u, err := uuid.Parse(val)
	if err != nil {
		return 0, isNull, errWrongValueForType.GenWithStackByArgs("string", val, "uuid_version")
	}
	return int64(u.Version()), false, nil
}

// Function: UUID_TIMESTAMP()
type uuidTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *uuidTimestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(18)
	bf.tp.SetDecimalUnderLimit(6)
	sig := &builtinUUIDTimestampSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UUIDTimestamp)
	return sig, nil
}

type builtinUUIDTimestampSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUUIDTimestampSig) Clone() builtinFunc {
	newSig := &builtinUUIDTimestampSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinUUIDTimestampSig.
func (b *builtinUUIDTimestampSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return new(types.MyDecimal), isNull, err
	}
	u, err := uuid.Parse(val)
	if err != nil {
		return new(types.MyDecimal), isNull, errWrongValueForType.GenWithStackByArgs("string", val, "uuid_timestamp")
	}

	switch u.Version() {
	case 1, 6, 7:
	default:
		// No timestamp, return NULL
		return new(types.MyDecimal), true, nil
	}

	s, ns := u.Time().UnixTime()
	r := new(types.MyDecimal)
	r.FromInt((s * 1000000) + (ns / 1000))
	err = r.Shift(-6)
	if err != nil {
		return new(types.MyDecimal), isNull, err
	}
	err = r.Round(r, 6, types.ModeHalfUp)
	if err != nil {
		return new(types.MyDecimal), isNull, err
	}

	return r, false, nil
}

type uuidShortFunctionClass struct {
	baseFunctionClass
}

func (c *uuidShortFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	// This should be:
	//
	// (server_id & 255) << 56
	// + (server_startup_time_in_seconds << 24)
	// + incremented_variable++;
	//
	// But we have a noop server_id
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// MySQL's UUID_TO_BIN is strict and doesn't trim spaces, unlike Go's uuid.Parse
	// We need to check if the string has leading/trailing spaces before parsing
	if strings.TrimSpace(val) != val {
		return "", false, errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
