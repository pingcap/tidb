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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, Decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"math"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

var (
	_ functionClass = &castFunctionClass{}
	_ functionClass = &castAsIntFunctionClass{}
	_ functionClass = &castAsRealFunctionClass{}
	_ functionClass = &castAsStringFunctionClass{}
	_ functionClass = &castAsDecimalFunctionClass{}
	_ functionClass = &castAsTimeFunctionClass{}
	_ functionClass = &castAsDurationFunctionClass{}
)

var (
	_ builtinFunc = &builtinCastSig{}

	_ builtinFunc = &builtinCastIntAsIntSig{}
	_ builtinFunc = &builtinCastIntAsRealSig{}
	_ builtinFunc = &builtinCastIntAsStringSig{}
	_ builtinFunc = &builtinCastIntAsDecimalSig{}
	_ builtinFunc = &builtinCastIntAsTimeSig{}
	_ builtinFunc = &builtinCastIntAsDurationSig{}
	_ builtinFunc = &builtinCastIntAsJSONSig{}

	_ builtinFunc = &builtinCastRealAsIntSig{}
	_ builtinFunc = &builtinCastRealAsRealSig{}
	_ builtinFunc = &builtinCastRealAsStringSig{}
	_ builtinFunc = &builtinCastRealAsDecimalSig{}
	_ builtinFunc = &builtinCastRealAsTimeSig{}
	_ builtinFunc = &builtinCastRealAsDurationSig{}
	_ builtinFunc = &builtinCastRealAsJSONSig{}

	_ builtinFunc = &builtinCastDecimalAsIntSig{}
	_ builtinFunc = &builtinCastDecimalAsRealSig{}
	_ builtinFunc = &builtinCastDecimalAsStringSig{}
	_ builtinFunc = &builtinCastDecimalAsDecimalSig{}
	_ builtinFunc = &builtinCastDecimalAsTimeSig{}
	_ builtinFunc = &builtinCastDecimalAsDurationSig{}
	_ builtinFunc = &builtinCastDecimalAsJSONSig{}

	_ builtinFunc = &builtinCastStringAsIntSig{}
	_ builtinFunc = &builtinCastStringAsRealSig{}
	_ builtinFunc = &builtinCastStringAsStringSig{}
	_ builtinFunc = &builtinCastStringAsDecimalSig{}
	_ builtinFunc = &builtinCastStringAsTimeSig{}
	_ builtinFunc = &builtinCastStringAsDurationSig{}
	_ builtinFunc = &builtinCastStringAsJSONSig{}

	_ builtinFunc = &builtinCastTimeAsIntSig{}
	_ builtinFunc = &builtinCastTimeAsRealSig{}
	_ builtinFunc = &builtinCastTimeAsStringSig{}
	_ builtinFunc = &builtinCastTimeAsDecimalSig{}
	_ builtinFunc = &builtinCastTimeAsTimeSig{}
	_ builtinFunc = &builtinCastTimeAsDurationSig{}
	_ builtinFunc = &builtinCastTimeAsJSONSig{}

	_ builtinFunc = &builtinCastDurationAsIntSig{}
	_ builtinFunc = &builtinCastDurationAsRealSig{}
	_ builtinFunc = &builtinCastDurationAsStringSig{}
	_ builtinFunc = &builtinCastDurationAsDecimalSig{}
	_ builtinFunc = &builtinCastDurationAsTimeSig{}
	_ builtinFunc = &builtinCastDurationAsDurationSig{}
	_ builtinFunc = &builtinCastDurationAsJSONSig{}

	_ builtinFunc = &builtinCastJSONAsIntSig{}
	_ builtinFunc = &builtinCastJSONAsRealSig{}
	_ builtinFunc = &builtinCastJSONAsStringSig{}
	_ builtinFunc = &builtinCastJSONAsDecimalSig{}
	_ builtinFunc = &builtinCastJSONAsTimeSig{}
	_ builtinFunc = &builtinCastJSONAsDurationSig{}
	_ builtinFunc = &builtinCastJSONAsJSONSig{}
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
		mysql.TypeDate, mysql.TypeLonglong, mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeJSON:
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
	bf := baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastIntAsIntSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsIntSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsIntSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsIntSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsIntSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsIntSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsIntSig{bf}
		} else {
			sig = &builtinCastStringAsIntSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsRealFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseRealBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastRealAsRealSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsRealSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsRealSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsRealSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsRealSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsRealSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsRealSig{bf}
		} else {
			sig = &builtinCastStringAsRealSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsDecimalFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseDecimalBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastDecimalAsDecimalSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsDecimalSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsDecimalSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsDecimalSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsDecimalSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsDecimalSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsDecimalSig{bf}
		} else {
			sig = &builtinCastStringAsDecimalSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsStringFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastStringAsStringSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsStringSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsStringSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsStringSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsStringSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsStringSig{bf}
		} else {
			sig = &builtinCastStringAsStringSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseTimeBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastTimeAsTimeSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsTimeSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsTimeSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsTimeSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsTimeSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsTimeSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsTimeSig{bf}
		} else {
			sig = &builtinCastStringAsTimeSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsDurationFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := baseDurationBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastDurationAsDurationSig{bf}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsDurationSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsDurationSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsDurationSig{bf}
	case types.ClassString:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsDurationSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsDurationSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsDurationSig{bf}
		} else {
			sig = &builtinCastStringAsDurationSig{bf}
		}
	}
	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

type castAsJSONFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (b *castAsJSONFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err := b.verifyArgs(args); err != nil {
		return sig, errors.Trace(err)
	}
	bf := baseJSONBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = b.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastJSONAsJSONSig{bf}
		return sig.setSelf(sig), nil
	}
	switch args[0].GetTypeClass() {
	case types.ClassInt:
		sig = &builtinCastIntAsJSONSig{bf}
	case types.ClassReal:
		sig = &builtinCastRealAsJSONSig{bf}
	case types.ClassDecimal:
		sig = &builtinCastDecimalAsJSONSig{bf}
	case types.ClassString:
		if tp := args[0].GetType().Tp; types.IsTypeTime(tp) {
			sig = &builtinCastTimeAsJSONSig{bf}
		} else if tp == mysql.TypeJSON {
			sig = &builtinCastJSONAsJSONSig{bf}
		} else if tp == mysql.TypeDuration {
			sig = &builtinCastDurationAsJSONSig{bf}
		} else {
			sig = &builtinCastStringAsJSONSig{bf}
		}
	}
	return sig.setSelf(sig), nil
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
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = float64(val)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		res = float64(uVal)
	}
	return res, false, errors.Trace(err)
}

type builtinCastIntAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = types.NewDecFromInt(val)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		if err != nil {
			return res, false, errors.Trace(err)
		}
		res = types.NewDecFromUint(uVal)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
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
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = strconv.FormatInt(val, 10)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		if err != nil {
			return res, false, errors.Trace(err)
		}
		res = strconv.FormatUint(uVal, 10)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc)
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
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
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

type builtinCastIntAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if mysql.HasUnsignedFlag(b.tp.Flag) {
		res = json.CreateJSON(uint64(val))
	} else {
		res = json.CreateJSON(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return json.CreateJSON(val), isNull, errors.Trace(err)
}

type builtinCastDecimalAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `STRING` now.
	return json.CreateJSON(string(val.ToString())), isNull, errors.Trace(err)
}

type builtinCastStringAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if b.tp.Decimal == 0 {
		res, err = json.ParseFromString(val)
	} else {
		// This is a post-wrapped cast.
		res = json.CreateJSON(val)
	}
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	val.Fsp = types.MaxFsp
	return json.CreateJSON(val.String()), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if val.Type == mysql.TypeDatetime || val.Type == mysql.TypeTimestamp {
		val.Fsp = types.MaxFsp
	}
	return json.CreateJSON(val.String()), false, nil
}

type builtinCastRealAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastRealAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	return b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastRealAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastRealAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if !mysql.HasUnsignedFlag(b.tp.Flag) {
		res, err = types.ConvertFloatToInt(sc, val, types.SignedLowerBound[mysql.TypeLonglong], types.SignedUpperBound[mysql.TypeLonglong], mysql.TypeDouble)
	} else {
		var uintVal uint64
		uintVal, err = types.ConvertFloatToUint(sc, val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeDouble)
		res = int64(uintVal)
	}
	return res, isNull, errors.Trace(err)
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
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
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
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	val.Round(&to, 0, types.ModeHalfEven)

	if mysql.HasUnsignedFlag(b.tp.Flag) {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	} else {
		res, err = to.ToInt()
	}

	if terror.ErrorEqual(err, types.ErrOverflow) {
		warnErr := types.ErrTruncatedWrongVal.GenByArgs("DECIMAL", val)
		err = sc.HandleOverflow(err, warnErr)
	}

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
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
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

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (b *builtinCastStringAsIntSig) handleOverflow(origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	sc := b.getCtx().GetSessionVars().StmtCtx
	if sc.InSelectStmt && terror.ErrorEqual(origErr, types.ErrOverflow) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenByArgs("INTEGER", origStr)
		err = sc.HandleOverflow(origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalInt(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	if isNegative {
		res, err = types.StrToInt(sc, val)
		if err == nil {
			// If overflow, don't append this warnings
			sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	} else {
		ures, err = types.StrToUint(sc, val)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.tp.Flag) && ures > uint64(math.MaxInt64) {
			sc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	}

	res, err = b.handleOverflow(res, val, err, isNegative)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastStringAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalReal(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.StrToFloat(sc, val)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp, sc)
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
	err = sc.HandleTruncate(res.FromString([]byte(val)))
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
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(val, b.tp.Decimal)
	if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
		err = sc.HandleTruncate(err)
	}
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
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
		res.Type = b.tp.Tp
	}
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
	t, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = t.ToNumber().ToInt()
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
	dur, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = dur.ToNumber().ToInt()
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

type builtinCastJSONAsJSONSig struct {
	baseJSONBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	return b.args[0].EvalJSON(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinCastJSONAsIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinCastJSONAsIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.CastToInt()
	return
}

type builtinCastJSONAsRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinCastJSONAsRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.CastToReal()
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(row []types.Datum) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	f64, err := val.CastToReal()
	if err == nil {
		res = new(types.MyDecimal)
		err = res.FromFloat64(f64)
	}
	return res, false, errors.Trace(err)
}

type builtinCastJSONAsStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.String(), false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ParseTime(s, b.tp.Tp, b.tp.Decimal)
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return
}

type builtinCastJSONAsDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ParseDuration(s, b.tp.Decimal)
	if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
		err = sc.HandleTruncate(err)
	}
	return
}

func buildCastFunction(expr Expression, tp *types.FieldType, ctx context.Context) (*ScalarFunction, error) {
	var fc functionClass
	switch tp.ToClass() {
	case types.ClassInt:
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ClassDecimal:
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ClassReal:
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ClassString:
		if types.IsTypeTime(tp.Tp) {
			fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		} else if tp.Tp == mysql.TypeDuration {
			fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		} else if tp.Tp == mysql.TypeJSON {
			fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		} else {
			fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		}
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
	tp.Flen, tp.Decimal = expr.GetType().Flen, 0
	types.SetBinChsClnFlag(tp)
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type
// of expr is not type real,
// otherwise, returns `expr` directly.
func WrapWithCastAsReal(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassReal {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type
// of expr is not type decimal,
// otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassDecimal {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.Flen, tp.Decimal = expr.GetType().Flen, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type
// of expr is not type string,
// otherwise, returns `expr` directly.
func WrapWithCastAsString(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetTypeClass() == types.ClassString {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.Charset, tp.Collate = charset.CharsetUTF8, charset.CollationUTF8
	tp.Flen, tp.Decimal = expr.GetType().Flen, types.UnspecifiedLength
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type
// of expr is not same as type of the specified `tp` ,
// otherwise, returns `expr` directly.
func WrapWithCastAsTime(expr Expression, tp *types.FieldType, ctx context.Context) (Expression, error) {
	if expr.GetType().Tp == tp.Tp {
		return expr, nil
	}
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate, mysql.TypeDuration:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	switch tp.Tp {
	case mysql.TypeDate:
		tp.Flen = mysql.MaxDateWidth
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.Flen = mysql.MaxDatetimeWidthNoFsp
		if tp.Decimal > 0 {
			tp.Flen = tp.Flen + 1 + tp.Decimal
		}
	}
	types.SetBinChsClnFlag(tp)
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type
// of expr is not type duration,
// otherwise, returns `expr` directly.
func WrapWithCastAsDuration(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetType().Tp == mysql.TypeDuration {
		return expr, nil
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	tp.Flen = mysql.MaxDurationWidthNoFsp
	if tp.Decimal > 0 {
		tp.Flen = tp.Flen + 1 + tp.Decimal
	}
	return buildCastFunction(expr, tp, ctx)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type
// of expr is not type json,
// otherwise, returns `expr` directly.
func WrapWithCastAsJSON(expr Expression, ctx context.Context) (Expression, error) {
	if expr.GetType().Tp == mysql.TypeJSON {
		return expr, nil
	}
	tp := &types.FieldType{
		Tp:   mysql.TypeJSON,
		Flen: 12582912,
		// Here we set decimal to -1 to indicate this is a post-wrapped cast.
		Decimal: -1,
		Charset: charset.CharsetUTF8,
		Collate: charset.CollationUTF8,
		Flag:    mysql.BinaryFlag,
	}
	return buildCastFunction(expr, tp, ctx)
}
