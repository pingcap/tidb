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
// See the License for the specific language governing permissions and
// limitations under the License.

// We implement CastXXAsYY built-in function signatures in this file.
// XX and YY contain the following types:
// Int, Decimal, Real, String, Time, Duration.
// For every type, we implement 5 signatures to cast it as the other 5 types.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &castFunctionClass{}
)

var (
	_ builtinFunc = &builtinCastSig{}
)

type castFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCastSig{newBaseBuiltinFunc(args, ctx), c.tp}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

// old built-in cast signature, will be removed later.
type builtinCastSig struct {
	baseBuiltinFunc

	tp *types.FieldType
}

// eval evals a builtinCastSig.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
// CastFuncFactory produces builtin function according to field types.
func (b *builtinCastSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	switch b.tp.Tp {
	// Parser has restricted this.
	// TypeDouble is used during plan optimization.
	case mysql.TypeString, mysql.TypeDuration, mysql.TypeDatetime,
		mysql.TypeDate, mysql.TypeLonglong, mysql.TypeNewDecimal, mysql.TypeDouble:
		d = args[0]
		if d.IsNull() {
			return
		}
		return d.ConvertTo(b.ctx.GetSessionVars().StmtCtx, b.tp)
	}
	return d, errors.Errorf("unknown cast type - %v", b.tp)
}

type builtinCastIntAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastIntAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	return b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastIntAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastIntAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	return float64(val), isNull, errors.Trace(err)
}

type builtinCastIntAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	return types.NewDecFromInt(val), isNull, errors.Trace(err)
}

type builtinCastIntAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastIntAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	return strconv.FormatInt(val, 10), isNull, errors.Trace(err)
}

type builtinCastIntAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTimeFromInt64(val)
	return res, false, errors.Trace(err)
}

type builtinCastIntAsDurationSig struct {
	baseDurationBuiltinFunc
	fsp int
}

func (b *builtinCastIntAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(strconv.FormatInt(val, 10), b.fsp)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastRealAsIntSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	return b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastRealAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastRealAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	return int64(val), isNull, errors.Trace(err)
}

type builtinCastRealAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = res.FromFloat64(val)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastRealAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	return strconv.FormatFloat(val, 'f', -1, 64), isNull, errors.Trace(err)
}

type builtinCastRealAsTimeSig struct {
	baseTimeBuiltinFunc
	tp  byte
	fsp int
}

func (b *builtinCastRealAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	res, err = types.ParseTime(strconv.FormatFloat(val, 'f', -1, 64), b.tp, b.fsp)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsDurationSig struct {
	baseDurationBuiltinFunc
	fsp int
}

func (b *builtinCastRealAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(strconv.FormatFloat(val, 'f', -1, 64), b.fsp)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	return b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastDecimalAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastDecimalAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToInt()
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return string(val.ToString()), false, nil
}

type builtinCastDecimalAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastDecimalAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsTimeSig struct {
	baseTimeBuiltinFunc
	tp byte
}

func (b *builtinCastDecimalAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	_, fsp := val.PrecisionAndFrac()
	res, err = types.ParseTime(string(val.ToString()), b.tp, fsp)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, false, errors.Trace(err)
	}
	_, fsp := val.PrecisionAndFrac()
	res, err = types.ParseDuration(string(val.ToString()), fsp)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastStringAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	return b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastStringAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastStringAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	if types.IsHybridType(b.args[0].GetType().Tp) {
		return b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	}
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = strconv.ParseInt(val, 10, 64)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastStringAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	if types.IsHybridType(b.args[0].GetType().Tp) {
		return b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	}
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = strconv.ParseFloat(val, 64)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	if types.IsHybridType(b.args[0].GetType().Tp) {
		return b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	}
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = res.FromString([]byte(val))
	return res, false, errors.Trace(err)
}

type builtinCastStringAsTimeSig struct {
	baseTimeBuiltinFunc
	tp  byte
	fsp int
}

func (b *builtinCastStringAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(val, b.tp, b.fsp)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDurationSig struct {
	baseDurationBuiltinFunc
	fsp int
}

func (b *builtinCastStringAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(val, b.fsp)
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) evalInt(row []types.Datum) (res types.Time, isNull bool, err error) {
	return b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastTimeAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastTimeAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToInt()
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastTimeAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.ToNumber(), false, errors.Trace(err)
}

type builtinCastTimeAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.String(), false, errors.Trace(err)
}

type builtinCastTimeAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ConvertToDuration()
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	return b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastDurationAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastDurationAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToInt()
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastDurationAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.ToNumber(), false, errors.Trace(err)
}

type builtinCastDurationAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.String(), false, errors.Trace(err)
}

type builtinCastDurationAsTimeSig struct {
	baseTimeBuiltinFunc
	tp byte
}

func (b *builtinCastDurationAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ConvertToTime(b.tp)
	return res, false, errors.Trace(err)
}
