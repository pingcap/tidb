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
	"math"
	"strings"
	gotime "time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)


func (b *builtinCastStringAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (*builtinCastStringAsIntSig) handleOverflow(ctx EvalContext, origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	ec := errCtx(ctx)
	if types.ErrOverflow.Equal(origErr) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", origStr)
		err = ec.HandleErrorWithAlias(origErr, origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	if b.args[0].GetType(ctx).Hybrid() || IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalInt(ctx, row)
	}

	// Take the implicit evalInt path if possible.
	if CanImplicitEvalInt(b.args[0]) {
		return b.args[0].EvalInt(ctx, row)
	}

	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	tc := typeCtx(ctx)
	if !isNegative {
		ures, err = types.StrToUint(tc, val, true)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.tp.GetFlag()) && ures > uint64(math.MaxInt64) {
			tc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	} else if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res = 0
	} else {
		res, err = types.StrToInt(tc, val, true)
		if err == nil && mysql.HasUnsignedFlag(b.tp.GetFlag()) {
			// If overflow, don't append this warnings
			tc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	}

	res, err = b.handleOverflow(ctx, res, val, err, isNegative)
	return res, false, err
}

type builtinCastStringAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalReal(ctx, row)
	}

	// Take the implicit evalReal path if possible.
	if CanImplicitEvalReal(b.args[0]) {
		return b.args[0].EvalReal(ctx, row)
	}

	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.StrToFloat(typeCtx(ctx), val, true)
	if err != nil {
		return 0, false, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp)
	return res, false, err
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalDecimal(ctx, row)
	}
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	val = strings.TrimSpace(val)
	isNegative := len(val) > 1 && val[0] == '-'
	res = new(types.MyDecimal)
	ec := errCtx(ctx)
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && isNegative) {
		err = res.FromString([]byte(val))
		if err == types.ErrTruncated {
			err = types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", []byte(val))
		}
		err = ec.HandleError(err)
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastStringAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTime(typeCtx(ctx), val, b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if res.IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, res.String()))
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastStringAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, isNull, err = types.ParseDuration(typeCtx(ctx), val, b.tp.GetDecimal())
	if types.ErrTruncatedWrongVal.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
	}
	return res, isNull, err
}

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)
	if res, err = res.Convert(tc, b.tp.GetType()); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		res.SetType(b.tp.GetType())
	}
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	t, err := val.RoundFrac(tc, types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if b.tp.GetType() == mysql.TypeYear {
		res, err = val.ConvertToYear(typeCtx(ctx))
	} else {
		var dur types.Duration
		dur, err = val.RoundFrac(types.DefaultFsp, location(ctx))
		if err != nil {
			return res, false, err
		}
		res, err = dur.ToNumber().ToInt()
	}
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx EvalContext) (string, bool, error) {
	flen := tp.GetFlen()
	if tp.GetType() == mysql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		maxAllowedPacket := ctx.GetMaxAllowedPacket()
		if uint64(flen) > maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "cast_as_binary", maxAllowedPacket)
		}
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	ts, err := getStmtTimestamp(ctx)
	if err != nil {
		ts = gotime.Now()
	}
	res, err = val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	return b.args[0].EvalJSON(ctx, row)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToInt64(typeCtx(ctx), val, mysql.HasUnsignedFlag(b.tp.GetFlag()))
	ec := errCtx(ctx)
	err = ec.HandleError(err)
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToFloat(typeCtx(ctx), val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ConvertJSONToDecimal(tc, val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceDecWithSpecifiedTp(tc, res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return s, false, nil
}

type builtinCastVectorFloat32AsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastVectorFloat32AsStringSig) Clone() builtinFunc {
	newSig := &builtinCastVectorFloat32AsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastVectorFloat32AsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return s, false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		res = val.GetTimeWithFsp(b.tp.GetDecimal())
		res.SetType(b.tp.GetType())
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		duration := val.GetDuration()

		tc := typeCtx(ctx)
		ts, err := getStmtTimestamp(ctx)
		if err != nil {
			ts = gotime.Now()
		}
		res, err = duration.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		res, err = res.RoundFrac(tc, b.tp.GetDecimal())
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, err = types.ParseTime(typeCtx(ctx), s, b.tp.GetType(), b.tp.GetDecimal())
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	default:
		ec := errCtx(ctx)
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs(types.TypeStr(b.tp.GetType()), val.String())
		return res, true, ec.HandleError(err)
	}
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		time := val.GetTimeWithFsp(b.tp.GetDecimal())
		res, err = time.ConvertToDuration()
		if err != nil {
			return res, false, err
		}
		res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		res = val.GetDuration()
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, _, err = types.ParseDuration(tc, s, b.tp.GetDecimal())
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
		}
		return res, isNull, err
	default:
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs("TIME", val.String())
		return res, true, tc.HandleTruncate(err)
	}
}

// CanImplicitEvalInt represents the builtin functions that have an implicit path to evaluate as integer,
// regardless of the type that type inference decides it to be.
// This is a nasty way to match the weird behavior of MySQL functions like `dayname()` being implicitly evaluated as integer.
