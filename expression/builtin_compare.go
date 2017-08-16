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
	"github.com/pingcap/tidb/util/types/json"
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

	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}

	_ builtinFunc = &builtinNullEQIntSig{}
	_ builtinFunc = &builtinNullEQRealSig{}
	_ builtinFunc = &builtinNullEQDecimalSig{}
	_ builtinFunc = &builtinNullEQStringSig{}
	_ builtinFunc = &builtinNullEQTimeSig{}
	_ builtinFunc = &builtinNullEQDurationSig{}
)

type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCoalesceSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinGreatestSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLeastSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinIntervalSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		return false
	}
	return true
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	ft0, ft1 := args[0].GetType(), args[1].GetType()
	tc0, tc1 := ft0.ToClass(), ft1.ToClass()
	cmpType := getCmpType(tc0, tc1)
	if (tc0 == types.ClassString && ft1.Tp == mysql.TypeJSON) ||
		(ft0.Tp == mysql.TypeJSON && tc1 == types.ClassString) {
		sig, err = c.generateCmpSigs(args, tpJSON, ctx)
	} else if cmpType == types.ClassString && (types.IsTypeTime(ft0.Tp) || types.IsTypeTime(ft1.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		sig, err = c.generateCmpSigs(args, tpTime, ctx)
	} else if ft0.Tp == mysql.TypeDuration && ft1.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		sig, err = c.generateCmpSigs(args, tpDuration, ctx)
	} else if cmpType == types.ClassReal || cmpType == types.ClassString {
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
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isColumn0 := args[0].(*Column)
			if !isColumn0 {
				col = args[1].(*Column)
			}
			if col.GetType().Tp == mysql.TypeDuration {
				sig, err = c.generateCmpSigs(args, tpDuration, ctx)
			} else {
				sig, err = c.generateCmpSigs(args, tpTime, ctx)
			}
		}
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if sig == nil {
		switch cmpType {
		case types.ClassString:
			sig, err = c.generateCmpSigs(args, tpString, ctx)
		case types.ClassInt:
			sig, err = c.generateCmpSigs(args, tpInt, ctx)
		case types.ClassDecimal:
			sig, err = c.generateCmpSigs(args, tpDecimal, ctx)
		case types.ClassReal:
			sig, err = c.generateCmpSigs(args, tpReal, ctx)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return sig.setSelf(sig), nil
}

// genCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(args []Expression, tp evalTp, ctx context.Context) (sig builtinFunc, err error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tp, tp)
	if err != nil {
		return sig, errors.Trace(err)
	}
	bf.tp.Flen = 1
	intBf := baseIntBuiltinFunc{bf}
	switch tp {
	case tpInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{intBf}
		case opcode.LE:
			sig = &builtinLEIntSig{intBf}
		case opcode.GT:
			sig = &builtinGTIntSig{intBf}
		case opcode.EQ:
			sig = &builtinEQIntSig{intBf}
		case opcode.GE:
			sig = &builtinGEIntSig{intBf}
		case opcode.NE:
			sig = &builtinNEIntSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{intBf}
		}
	case tpReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{intBf}
		case opcode.LE:
			sig = &builtinLERealSig{intBf}
		case opcode.GT:
			sig = &builtinGTRealSig{intBf}
		case opcode.GE:
			sig = &builtinGERealSig{intBf}
		case opcode.EQ:
			sig = &builtinEQRealSig{intBf}
		case opcode.NE:
			sig = &builtinNERealSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQRealSig{intBf}
		}
	case tpDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{intBf}
		case opcode.LE:
			sig = &builtinLEDecimalSig{intBf}
		case opcode.GT:
			sig = &builtinGTDecimalSig{intBf}
		case opcode.GE:
			sig = &builtinGEDecimalSig{intBf}
		case opcode.EQ:
			sig = &builtinEQDecimalSig{intBf}
		case opcode.NE:
			sig = &builtinNEDecimalSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQDecimalSig{intBf}
		}
	case tpString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{intBf}
		case opcode.LE:
			sig = &builtinLEStringSig{intBf}
		case opcode.GT:
			sig = &builtinGTStringSig{intBf}
		case opcode.GE:
			sig = &builtinGEStringSig{intBf}
		case opcode.EQ:
			sig = &builtinEQStringSig{intBf}
		case opcode.NE:
			sig = &builtinNEStringSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{intBf}
		}
	case tpDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{intBf}
		case opcode.LE:
			sig = &builtinLEDurationSig{intBf}
		case opcode.GT:
			sig = &builtinGTDurationSig{intBf}
		case opcode.GE:
			sig = &builtinGEDurationSig{intBf}
		case opcode.EQ:
			sig = &builtinEQDurationSig{intBf}
		case opcode.NE:
			sig = &builtinNEDurationSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQDurationSig{intBf}
		}
	case tpTime:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{intBf}
		case opcode.LE:
			sig = &builtinLETimeSig{intBf}
		case opcode.GT:
			sig = &builtinGTTimeSig{intBf}
		case opcode.GE:
			sig = &builtinGETimeSig{intBf}
		case opcode.EQ:
			sig = &builtinEQTimeSig{intBf}
		case opcode.NE:
			sig = &builtinNETimeSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQTimeSig{intBf}
		}
	case tpJSON:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{intBf}
		case opcode.LE:
			sig = &builtinLEJSONSig{intBf}
		case opcode.GT:
			sig = &builtinGTJSONSig{intBf}
		case opcode.GE:
			sig = &builtinGEJSONSig{intBf}
		case opcode.EQ:
			sig = &builtinEQJSONSig{intBf}
		case opcode.NE:
			sig = &builtinNEJSONSig{intBf}
		case opcode.NullEQ:
			sig = &builtinNullEQJSONSig{intBf}
		}
	}
	return
}

type builtinLTIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareInt(s.args, row, s.ctx))
}

type builtinLTRealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTRealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareReal(s.args, row, s.ctx))
}

type builtinLTDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareDecimal(s.args, row, s.ctx))
}

type builtinLTStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareString(s.args, row, s.ctx))
}

type builtinLTDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareDuration(s.args, row, s.ctx))
}

type builtinLTTimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTTimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareTime(s.args, row, s.ctx))
}

type builtinLTJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLTJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLT(compareJSON(s.args, row, s.ctx))
}

type builtinLEIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLEIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareInt(s.args, row, s.ctx))
}

type builtinLERealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLERealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareReal(s.args, row, s.ctx))
}

type builtinLEDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLEDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareDecimal(s.args, row, s.ctx))
}

type builtinLEStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLEStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareString(s.args, row, s.ctx))
}

type builtinLEDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLEDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareDuration(s.args, row, s.ctx))
}

type builtinLETimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLETimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareTime(s.args, row, s.ctx))
}

type builtinLEJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinLEJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfLE(compareJSON(s.args, row, s.ctx))
}

type builtinGTIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareInt(s.args, row, s.ctx))
}

type builtinGTRealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTRealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareReal(s.args, row, s.ctx))
}

type builtinGTDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareDecimal(s.args, row, s.ctx))
}

type builtinGTStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareString(s.args, row, s.ctx))
}

type builtinGTDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareDuration(s.args, row, s.ctx))
}

type builtinGTTimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTTimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareTime(s.args, row, s.ctx))
}

type builtinGTJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGTJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGT(compareJSON(s.args, row, s.ctx))
}

type builtinGEIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGEIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareInt(s.args, row, s.ctx))
}

type builtinGERealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGERealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareReal(s.args, row, s.ctx))
}

type builtinGEDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGEDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareDecimal(s.args, row, s.ctx))
}

type builtinGEStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGEStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareString(s.args, row, s.ctx))
}

type builtinGEDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGEDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareDuration(s.args, row, s.ctx))
}

type builtinGETimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGETimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareTime(s.args, row, s.ctx))
}

type builtinGEJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinGEJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfGE(compareJSON(s.args, row, s.ctx))
}

type builtinEQIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareInt(s.args, row, s.ctx))
}

type builtinEQRealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQRealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareReal(s.args, row, s.ctx))
}

type builtinEQDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareDecimal(s.args, row, s.ctx))
}

type builtinEQStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareString(s.args, row, s.ctx))
}

type builtinEQDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareDuration(s.args, row, s.ctx))
}

type builtinEQTimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQTimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareTime(s.args, row, s.ctx))
}

type builtinEQJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinEQJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfEQ(compareJSON(s.args, row, s.ctx))
}

type builtinNEIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNEIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareInt(s.args, row, s.ctx))
}

type builtinNERealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNERealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareReal(s.args, row, s.ctx))
}

type builtinNEDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNEDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareDecimal(s.args, row, s.ctx))
}

type builtinNEStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNEStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareString(s.args, row, s.ctx))
}

type builtinNEDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNEDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareDuration(s.args, row, s.ctx))
}

type builtinNETimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNETimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareTime(s.args, row, s.ctx))
}

type builtinNEJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNEJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	return resOfNE(compareJSON(s.args, row, s.ctx))
}

type builtinNullEQIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalInt(row, sc)
	if err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalInt(row, sc)
	if err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(s.args[0].GetType().Flag), mysql.HasUnsignedFlag(s.args[1].GetType().Flag)
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		res = 1
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQRealSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalReal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQDecimalSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQStringSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQStringSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalString(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareString(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQDurationSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalDuration(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDuration(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQTimeSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalTime(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalTime(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	baseIntBuiltinFunc
}

func (s *builtinNullEQJSONSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := s.args[0].EvalJSON(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalJSON(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	default:
		cmpRes, err := json.CompareJSON(arg0, arg1)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func compareInt(args []Expression, row []types.Datum, ctx context.Context) (val int64, isNull bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalInt(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalInt(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(args[0].GetType().Flag), mysql.HasUnsignedFlag(args[1].GetType().Flag)
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
	return int64(res), false, nil
}

func compareString(args []Expression, row []types.Datum, ctx context.Context) (val int64, isNull bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalString(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalString(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	return int64(types.CompareString(arg0, arg1)), false, nil
}

func compareReal(args []Expression, row []types.Datum, ctx context.Context) (val int64, isNull bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalReal(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalReal(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

func compareDecimal(args []Expression, row []types.Datum, ctx context.Context) (val int64, isNull bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalDecimal(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalDecimal(row, sc)
	if err != nil {
		return zeroI64, false, errors.Trace(err)
	}
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareTime(args []Expression, row []types.Datum, ctx context.Context) (int64, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalTime(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalTime(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareDuration(args []Expression, row []types.Datum, ctx context.Context) (int64, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalDuration(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalDuration(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareJSON(args []Expression, row []types.Datum, ctx context.Context) (int64, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := args[0].EvalJSON(row, sc)
	if isNull0 || err != nil {
		return zeroI64, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalJSON(row, sc)
	if isNull1 || err != nil {
		return zeroI64, isNull1, errors.Trace(err)
	}
	res, err := json.CompareJSON(arg0, arg1)
	return int64(res), false, errors.Trace(err)
}
