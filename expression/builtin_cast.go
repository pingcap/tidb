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
// For every type, we implement 6 signatures to cast it as all the 6 types.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
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

// builtinCastSig is old built-in cast signature and will be removed later.
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

type castAsIntFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsIntFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseIntBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastIntAsIntSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastIntAsIntSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsIntSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsIntSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsIntSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsIntSig{bf}
		default:
			sig = builtinCastStringAsIntSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsRealFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseRealBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastRealAsRealSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastIntAsRealSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsRealSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsRealSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsRealSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsRealSig{bf}
		default:
			sig = builtinCastStringAsRealSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsDecimalFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseDecimalBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastDecimalAsDecimalSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastIntAsDecimalSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsDecimalSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsDecimalSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsDecimalSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsDecimalSig{bf}
		default:
			sig = builtinCastStringAsDecimalSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsStringFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseStringBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastStringAsStringSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastIntAsStringSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsStringSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsStringSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsStringSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsStringSig{bf}
		default:
			sig = builtinCastStringAsStringSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseTimeBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastTimeAsTimeSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastTimeAsTimeSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsTimeSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsTimeSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsTimeSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsTimeSig{bf}
		default:
			sig = builtinCastStringAsTimeSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsDurationFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	// todo: change newBaseBuiltinFunc tp newBaseBuiltinFuncWithTp
	bf := baseDurationBuiltinFunc{newBaseBuiltinFuncWithTp(args, b.tp, ctx)}
	if IsHybridType(args[0]) {
		return builtinCastDurationAsDurationSig{bf}, nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = builtinCastIntAsDurationSig{bf}
	case types.ClassReal:
		sig = builtinCastRealAsDurationSig{bf}
	case types.ClassDecimal:
		sig = builtinCastDecimalAsDurationSig{bf}
	case types.ClassString:
		switch args[0].GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = builtinCastTimeAsDurationSig{bf}
		case mysql.TypeDuration:
			sig = builtinCastDurationAsDurationSig{bf}
		default:
			sig = builtinCastStringAsDurationSig{bf}
		}
	}
	return bf, errors.Trace(b.verifyArgs(args))
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	res, err = types.ProduceDecWithSpecifiedTp(types.NewDecFromInt(val), b.tp, sc)
	return res, isNull, errors.Trace(err)
}

type builtinCastIntAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastIntAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatInt(val, 10), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastIntAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(strconv.FormatInt(val, 10), b.tp.Tp, b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastIntAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(strconv.FormatInt(val, 10), b.tp.Decimal)
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = res.FromFloat64(val)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastRealAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, 64), b.tp, sc)
	return res, isNull, errors.Trace(err)
}

type builtinCastRealAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	res, err = types.ParseTime(strconv.FormatFloat(val, 'f', -1, 64), b.tp.Tp, b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(strconv.FormatFloat(val, 'f', -1, 64), b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, sc)
	return res, false, errors.Trace(err)
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
}

func (b *builtinCastDecimalAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(string(val.ToString()), b.tp.Tp, b.tp.Decimal)
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
	res, err = types.ParseDuration(string(val.ToString()), b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastStringAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastStringAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	if IsHybridType(b.args[0]) {
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
	if IsHybridType(b.args[0]) {
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalDecimal(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = res.FromString([]byte(val))
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(val, b.tp.Tp, b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(val, b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc)
	return res, false, errors.Trace(err)
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
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ConvertToTime(b.tp.Tp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type castType byte

const (
	castAsInt castType = iota
	castAsDecimal
	castAsString
	castAsReal
	castAsTime
	castAsDuration
)

func buildCastFunction(expr Expression, tp *types.FieldType, target castType, ctx context.Context) (*ScalarFunction, error) {
	var fc functionClass
	switch target {
	case castAsInt:
		fc = castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case castAsDecimal:
		fc = castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case castAsString:
		fc = castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case castAsReal:
		fc = castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case castAsTime:
		fc = castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case castAsDuration:
		fc = castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	}
	f, err := fc.getFunction([]Expression{expr}, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}, nil
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type
// of expr is not type int,
// otherwise, returns `expr` directly.
func WrapWithCastAsInt(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassInt {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	return buildCastFunction(expr, tp, castAsInt, ctx)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type
// of expr is not type real,
// otherwise, returns `expr` directly.
func WrapWithCastAsReal(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassReal {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	return buildCastFunction(expr, tp, castAsReal, ctx)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type
// of expr is not type decimal,
// otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassDecimal {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	return buildCastFunction(expr, tp, castAsDecimal, ctx)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type
// of expr is not type string,
// otherwise, returns `expr` directly.
func WrapWithCastAsString(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassString {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.Charset, tp.Collate = expr.GetType().Charset, expr.GetType().Collate
	return buildCastFunction(expr, tp, castAsString, ctx)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type
// of expr is not same as type of the specified `tp` ,
// otherwise, returns `expr` directly.
func WrapWithCastAsTime(expr Expression, tp *types.FieldType, ctx context.Context) (Expression, error) {
	if expr.GetType().Tp == tp.Tp {
		return expr, nil
	}
	switch x := expr.GetType(); x {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate, mysql.TypeDuration:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	return buildCastFunction(expr, tp, castAsTime, ctx)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type
// of expr is not type duration,
// otherwise, returns `expr` directly.
func WrapWithCastAsDuration(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetType().Tp == mysql.TypeDuration {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(); x {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	return buildCastFunction(expr, tp, castAsDuration, ctx)
}
