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

package expression

import (
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &coalesceFunctionClass{}
	_ functionClass = &greatestFunctionClass{}
	_ functionClass = &leastFunctionClass{}
	_ functionClass = &intervalFunctionClass{}
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinCoalesceSig{}
	_ builtinFunc = &builtinGreatestSig{}
	_ builtinFunc = &builtinLeastSig{}
	_ builtinFunc = &builtinIntervalSig{}
	_ builtinFunc = &builtinCompareIntSig{}
	_ builtinFunc = &builtinCompareRealSig{}
	_ builtinFunc = &builtinCompareDecimalSig{}
	_ builtinFunc = &builtinCompareStringSig{}
	_ builtinFunc = &builtinCompareTimeSig{}
	_ builtinFunc = &builtinCompareDurationSig{}
)

type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCoalesceSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCoalesceSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinCoalesce(args, b.ctx)
}

// builtinCoalesce returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func builtinCoalesce(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	for _, d = range args {
		if !d.IsNull() {
			return d, nil
		}
	}
	return d, nil
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinGreatestSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinGreatestSig struct {
	baseBuiltinFunc
}

// eval evals a builtinGreatestSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	max := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return
		}

		var cmp int
		if cmp, err = args[i].CompareDatum(sc, args[max]); err != nil {
			return
		}

		if cmp > 0 {
			max = i
		}
	}
	d = args[max]
	return
}

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLeastSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLeastSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLeastSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_least
func (b *builtinLeastSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	min := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return
		}

		var cmp int
		if cmp, err = args[i].CompareDatum(sc, args[min]); err != nil {
			return
		}

		if cmp < 0 {
			min = i
		}
	}
	d = args[min]
	return
}

type intervalFunctionClass struct {
	baseFunctionClass
}

func (c *intervalFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinIntervalSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinIntervalSig struct {
	baseBuiltinFunc
}

// eval evals a builtinIntervalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetInt64(int64(-1))
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx

	idx := sort.Search(len(args)-1, func(i int) bool {
		d1, d2 := args[0], args[i+1]
		if d1.Kind() == types.KindInt64 && d1.Kind() == d2.Kind() {
			return d1.GetInt64() < d2.GetInt64()
		}
		if d1.Kind() == types.KindUint64 && d1.Kind() == d2.Kind() {
			return d1.GetUint64() < d2.GetUint64()
		}
		if d1.Kind() == types.KindInt64 && d2.Kind() == types.KindUint64 {
			return d1.GetInt64() < 0 || d1.GetUint64() < d2.GetUint64()
		}
		if d1.Kind() == types.KindUint64 && d2.Kind() == types.KindInt64 {
			return d2.GetInt64() > 0 && d1.GetUint64() < d2.GetUint64()
		}
		v1, _ := d1.ToFloat64(sc)
		v2, _ := d2.ToFloat64(sc)
		return v1 < v2
	})
	d.SetInt64(int64(idx))

	return
}

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getCmpType gets the ClassType that the two args will be treated as when comparing.
func getCmpType(a types.TypeClass, b types.TypeClass) types.TypeClass {
	if a == types.ClassString && b == types.ClassString {
		return types.ClassString
	} else if a == types.ClassInt && b == types.ClassInt {
		return types.ClassInt
	} else if (a == types.ClassInt || a == types.ClassDecimal) &&
		(b == types.ClassInt || b == types.ClassDecimal) {
		return types.ClassDecimal
	}
	return types.ClassReal
}

// canCompareAsTime checks whether the two args can be compared as time.
// date[time] <cmp> date[time]
// string <cmp> date[time]
func canCompareAsTime(ft0, ft1 *types.FieldType) bool {
	if isTemporalTypeWithDate(ft0) && ft1.ToClass() == types.ClassString {
		return true
	}
	if isTemporalTypeWithDate(ft1) && ft0.ToClass() == types.ClassString { // string <cmp> date[time]
		return true
	}
	return false
}

// isTemporalTypeWithDate checks if ft is temporal type and has date part.
func isTemporalTypeWithDate(ft *types.FieldType) bool {
	return types.IsTypeTime(ft.Tp)
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if !isTemporalTypeWithDate(ft) && ft.Tp != mysql.TypeDuration {
		return false
	}
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	return true
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	// TODO: we do not support JSON in new expression evaluation architecture now.
	if args[0].GetType().Tp == mysql.TypeJSON || args[1].GetType().Tp == mysql.TypeJSON {
		bf := newBaseBuiltinFunc(args, ctx)
		bf.tp = &types.FieldType{Tp: mysql.TypeLonglong, Flen: 1, Decimal: 0}
		types.SetBinChsClnFlag(bf.tp)
		sig = &builtinCompareSig{bf, c.op}
		return sig.setSelf(sig), c.verifyArgs(args)
	}
	ft0, ft1 := args[0].GetType(), args[1].GetType()
	tc0, tc1 := ft0.ToClass(), ft1.ToClass()
	cmpType := getCmpType(tc0, tc1)
	var bf baseBuiltinFunc
	if cmpType == types.ClassString && (types.IsTypeTime(ft0.Tp) || types.IsTypeTime(ft1.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpTime, tpTime)
		sig = &builtinCompareTimeSig{baseIntBuiltinFunc{bf}, c.op}
	} else if ft0.Tp == mysql.TypeDuration && ft1.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration, tpDuration)
		sig = &builtinCompareDurationSig{baseIntBuiltinFunc{bf}, c.op}
	} else if cmpType == types.ClassReal {
		_, isConst0 := args[0].(*Constant)
		_, isConst1 := args[1].(*Constant)
		if (tc0 == types.ClassDecimal && !isConst0 && tc1 == types.ClassString && isConst1) ||
			(tc1 == types.ClassDecimal && !isConst1 && tc0 == types.ClassString && isConst0) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparision as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ClassDecimal
		} else if isTemporalColumn(args[0]) && isConst1 ||
			isTemporalColumn(args[1]) && isConst0 {
			/*
				<time column> <cmp> <non-time constant>
				or
				<non-time constant> <cmp> <time column>

				Convert the constant to time type.
			*/
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpTime, tpTime)
			sig = &builtinCompareTimeSig{baseIntBuiltinFunc{bf}, c.op}
		}
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if sig == nil {
		switch cmpType {
		case types.ClassString:
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpString)
			sig = &builtinCompareStringSig{baseIntBuiltinFunc{bf}, c.op}
		case types.ClassInt:
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
			sig = &builtinCompareIntSig{baseIntBuiltinFunc{bf}, c.op}
		case types.ClassDecimal:
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDecimal, tpDecimal)
			sig = &builtinCompareDecimalSig{baseIntBuiltinFunc{bf}, c.op}
		case types.ClassReal:
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpReal, tpReal)
			sig = &builtinCompareRealSig{baseIntBuiltinFunc{bf}, c.op}
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	bf.tp.Flen = 1
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCompareSig struct {
	baseBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}

	sc := s.ctx.GetSessionVars().StmtCtx
	var a, b = args[0], args[1]

	if a.IsNull() || b.IsNull() {
		// For <=>, if a and b are both nil, return true.
		// If a or b is nil, return false.
		if s.op == opcode.NullEQ {
			if a.IsNull() && b.IsNull() {
				d.SetInt64(oneI64)
			} else {
				d.SetInt64(zeroI64)
			}
		}
		return
	}

	if s.op != opcode.NullEQ {
		if aa, bb, err := types.CoerceDatum(sc, a, b); err == nil {
			a = aa
			b = bb
		}
	}

	n, err := a.CompareDatum(sc, b)
	if err != nil {
		// TODO: should deal with error here.
		return d, errors.Trace(err)
	}
	var result bool
	switch s.op {
	case opcode.LT:
		result = n < 0
	case opcode.LE:
		result = n <= 0
	case opcode.EQ, opcode.NullEQ:
		result = n == 0
	case opcode.GT:
		result = n > 0
	case opcode.GE:
		result = n >= 0
	case opcode.NE:
		result = n != 0
	default:
		return d, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	if result {
		d.SetInt64(oneI64)
	} else {
		d.SetInt64(zeroI64)
	}
	return
}

// resOfCmp returns the results of different compare built-in functions.
func resOfCmp(res int, op opcode.Op) int64 {
	var ret bool
	switch op {
	case opcode.LT:
		ret = res < 0
	case opcode.LE:
		ret = res <= 0
	case opcode.EQ, opcode.NullEQ:
		ret = res == 0
	case opcode.GT:
		ret = res > 0
	case opcode.GE:
		ret = res >= 0
	case opcode.NE:
		ret = res != 0
	default:
		return -1
	}
	res = 1
	if !ret {
		res = 0
	}
	return int64(res)
}

// builtinCompareIntSig compares two integers.
type builtinCompareIntSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalInt(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalInt(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull0 || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull0 && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(s.args[0].GetType().Flag), mysql.HasUnsignedFlag(s.args[1].GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	ret := resOfCmp(res, s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}

// builtinCompareStringSig compares two strings.
type builtinCompareStringSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull0 || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull0 && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	ret := resOfCmp(types.CompareString(arg0, arg1), s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}

// builtinCompareRealSig compares two reals.
type builtinCompareRealSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareRealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull0 || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull0 && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	ret := resOfCmp(types.CompareFloat64(arg0, arg1), s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}

// builtinCompareDecimalSig compares two decimals.
type builtinCompareDecimalSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := s.args[0].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	ret := resOfCmp(arg0.Compare(arg1), s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}

// builtinCompareTimeSig compares two time.
type builtinCompareTimeSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareTimeSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := s.args[0].EvalTime(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalTime(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	ret := resOfCmp(arg0.Compare(arg1), s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}

// builtinCompareDurationSig compares two duration.
type builtinCompareDurationSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareDurationSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := s.args[0].EvalDuration(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDuration(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull || isNull1 {
		if s.op == opcode.NullEQ {
			if isNull && isNull1 {
				return oneI64, false, nil
			}
			return zeroI64, false, nil
		}
		return zeroI64, true, nil
	}
	ret := resOfCmp(arg0.Compare(arg1), s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}
