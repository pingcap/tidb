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
	"github.com/pingcap/tidb/ast"
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
	_ builtinFunc = &builtinCompareSig{}
)

type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCoalesceSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinGreatestSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinLeastSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinIntervalSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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

func (c *compareFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCompareSig{newBaseBuiltinFunc(args, ctx), c.op}, errors.Trace(c.verifyArgs(args))
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
	if s.op != opcode.NullEQ {
		a, b, err = types.CoerceDatum(sc, a, b)
		if err != nil {
			return d, errors.Trace(err)
		}
	}
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

	n, err := a.CompareDatum(sc, b)
	if err != nil {
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

// builtinCompareStringSig compares two strings.
type builtinCompareStringSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareStringSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isKindNull0, err := s.args[0].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isKindNull1, err := s.args[1].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isKindNull0 || isKindNull1 {
		if s.op == opcode.NullEQ {
			if isKindNull0 && isKindNull1 {
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

func (s *builtinCompareRealSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isKindNull0, err := s.args[0].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isKindNull1, err := s.args[1].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isKindNull0 || isKindNull1 {
		if s.op == opcode.NullEQ {
			if isKindNull0 && isKindNull1 {
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

func (s *builtinCompareDecimalSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isKindNull0, err := s.args[0].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isKindNull1, err := s.args[1].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isKindNull0 || isKindNull1 {
		if s.op == opcode.NullEQ {
			if isKindNull0 && isKindNull1 {
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

// builtinCompareIntSig compares two integers.
type builtinCompareIntSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isKindNull0, err := s.args[0].EvalInt(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isKindNull1, err := s.args[1].EvalInt(row, sc)
	if err != nil {
		return zeroI64, isKindNull1, errors.Trace(err)
	}
	if isKindNull0 || isKindNull1 {
		if s.op == opcode.NullEQ {
			if isKindNull0 && isKindNull1 {
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

// builtinCompareRowSig compares two rows.
type builtinCompareRowSig struct {
	baseIntBuiltinFunc

	op opcode.Op
}

func (s *builtinCompareRowSig) evalInt(row []types.Datum) (int64, bool, error) {
	var row0, row1 []Expression
	if sf, ok := s.args[0].(*ScalarFunction); ok && sf.FuncName.O == ast.RowFunc {
		row0 = sf.GetArgs()
	} else {
		row0 = []Expression{s.args[0]}
	}
	if sf, ok := s.args[1].(*ScalarFunction); ok && sf.FuncName.O == ast.RowFunc {
		row1 = sf.GetArgs()
	} else {
		row1 = []Expression{s.args[1]}
	}
	res := 0
	for i := 0; i < len(row0) && i < len(row1); i++ {
		arg0, err := row0[i].Eval(row)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		arg1, err := row1[i].Eval(row)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		isKindNull0, isKindNull1 := arg0.IsNull(), arg1.IsNull()
		if isKindNull0 || isKindNull1 {
			if s.op == opcode.NullEQ {
				if isKindNull0 && isKindNull1 {
					return oneI64, false, nil
				}
				return zeroI64, false, nil
			}
			return zeroI64, true, nil
		}
		res, err = arg0.CompareDatum(s.getCtx().GetSessionVars().StmtCtx, arg1)
		if err != nil {
			return zeroI64, false, errors.Trace(err)
		}
		if res != 0 {
			break
		}
	}
	ret := resOfCmp(res, s.op)
	if ret == -1 {
		return zeroI64, false, errInvalidOperation.Gen("invalid op %v in comparison operation", s.op)
	}
	return ret, false, nil
}
