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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type builtinCastRealAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res, err = types.ConvertFloatToInt(val, types.IntegerSignedLowerBound(mysql.TypeLonglong), types.IntegerSignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
	} else if b.inUnion && val < 0 {
		res = 0
	} else {
		var uintVal uint64
		tc := typeCtx(ctx)
		uintVal, err = types.ConvertFloatToUint(tc.Flags(), val, types.IntegerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		res = int64(uintVal)
	}
	if types.ErrOverflow.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
	}
	return res, isNull, err
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = new(types.MyDecimal)
	ec := errCtx(ctx)
	if !b.inUnion || val >= 0 {
		err = res.FromFloat64(val)
		if types.ErrOverflow.Equal(err) {
			warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0].StringWithCtx(ctx, errors.RedactLogDisable))
			err = ec.HandleErrorWithAlias(err, err, warnErr)
		} else if types.ErrTruncated.Equal(err) {
			// This behavior is consistent with MySQL.
			err = nil
		}
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastRealAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	bits := 64
	if b.args[0].GetType(ctx).GetType() == mysql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}

	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, bits), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastRealAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	// MySQL compatibility: 0 should not be converted to null, see #11203
	fv := strconv.FormatFloat(val, 'f', -1, 64)
	if fv == "0" {
		return types.ZeroTime, false, nil
	}
	res, err := types.ParseTimeFromFloatString(typeCtx(ctx), fv, b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	res, _, err = types.ParseDuration(tc, strconv.FormatFloat(val, 'f', -1, 64), b.tp.GetDecimal())
	if err != nil {
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
			// ErrTruncatedWrongVal needs to be considered NULL.
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	evalDecimal, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = &types.MyDecimal{}
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && evalDecimal.IsNegative()) {
		*res = *evalDecimal
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	ec := errCtx(ctx)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfUp)
	if err != nil {
		return 0, true, err
	}

	if !mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res, err = to.ToInt()
	} else if b.inUnion && to.IsNegative() {
		res = 0
	} else {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	}

	if types.ErrOverflow.Equal(err) {
		ec := errCtx(ctx)
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = ec.HandleErrorWithAlias(err, err, warnErr)
	}

	return res, false, err
}

type builtinCastDecimalAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func setDataTypeDouble(srcDecimal int) (flen, decimal int) {
	decimal = mysql.NotFixedDec
	flen = floatLength(srcDecimal, decimal)
	return
}

func floatLength(srcDecimal int, decimalPar int) int {
	const dblDIG = 15
	if srcDecimal != mysql.NotFixedDec {
		return dblDIG + 2 + decimalPar
	}
	return dblDIG + 8
}

func (b *builtinCastDecimalAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && val.IsNegative() {
		res = 0
	} else {
		res, err = val.ToFloat64()
	}
	return res, false, err
}

type builtinCastDecimalAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTimeFromFloatString(typeCtx(ctx), string(val.ToString()), b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, err
}

type builtinCastDecimalAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, true, err
	}
	res, _, err = types.ParseDuration(typeCtx(ctx), string(val.ToString()), b.tp.GetDecimal())
	if types.ErrTruncatedWrongVal.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
		// ErrTruncatedWrongVal needs to be considered NULL.
		return res, true, err
	}
	return res, false, err
}

type builtinCastStringAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}
