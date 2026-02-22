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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type builtinCastIntAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	return
}

type builtinCastIntAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if unsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()); !mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 {
		res = float64(val)
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
		res = 0
	} else {
		// recall that, int to float is different from uint to float
		res = float64(uint64(val))
	}
	return res, false, err
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if unsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()); !mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 {
		//revive:disable:empty-lines
		res = types.NewDecFromInt(val)
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
		//revive:enable:empty-lines
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		res = &types.MyDecimal{}
	} else {
		res = types.NewDecFromUint(uint64(val))
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, res, b.tp)
	err = ec.HandleError(err)
	return res, isNull, err
}

type builtinCastIntAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tp := b.args[0].GetType(ctx)
	if !mysql.HasUnsignedFlag(tp.GetFlag()) {
		res = strconv.FormatInt(val, 10)
	} else {
		res = strconv.FormatUint(uint64(val), 10)
	}
	if tp.GetType() == mysql.TypeYear && res == "0" {
		res = "0000"
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastIntAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
		res, err = types.ParseTimeFromYear(val)
	} else {
		res, err = types.ParseTimeFromNum(typeCtx(ctx), val, b.tp.GetType(), b.tp.GetDecimal())
	}

	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := types.NumberToDuration(val, b.tp.GetDecimal())
	if err != nil {
		if types.ErrOverflow.Equal(err) || types.ErrTruncatedWrongVal.Equal(err) {
			ec := errCtx(ctx)
			err = ec.HandleError(err)
		}
		return res, true, err
	}
	return dur, false, err
}

type builtinCastIntAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastIntAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if mysql.HasIsBooleanFlag(b.args[0].GetType(ctx).GetFlag()) {
		res = types.CreateBinaryJSON(val != 0)
	} else if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
		res = types.CreateBinaryJSON(uint64(val))
	} else {
		res = types.CreateBinaryJSON(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return types.CreateBinaryJSON(val), isNull, err
}

type builtinCastDecimalAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDecimalAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return types.BinaryJSON{}, true, err
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	f64, err := val.ToFloat64()
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	return types.CreateBinaryJSON(f64), isNull, err
}

type builtinCastStringAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	typ := b.args[0].GetType(ctx)
	if types.IsBinaryStr(typ) {
		buf := []byte(val)
		if typ.GetType() == mysql.TypeString && typ.GetFlen() > 0 {
			// the tailing zero should also be in the opaque json
			buf = make([]byte, typ.GetFlen())
			copy(buf, val)
		}

		res := types.CreateBinaryJSON(types.Opaque{
			TypeCode: b.args[0].GetType(ctx).GetType(),
			Buf:      buf,
		})

		return res, false, err
	} else if mysql.HasParseToJSONFlag(b.tp.GetFlag()) {
		res, err = types.ParseBinaryJSONFromString(val)
	} else {
		res = types.CreateBinaryJSON(val)
	}
	return res, false, err
}

type builtinCastDurationAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	val.Fsp = types.MaxFsp
	return types.CreateBinaryJSON(val), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Type() == mysql.TypeDatetime || val.Type() == mysql.TypeTimestamp {
		val.SetFsp(types.MaxFsp)
	}
	return types.CreateBinaryJSON(val), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastRealAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalReal(ctx, row)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	return
}
