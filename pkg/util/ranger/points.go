// Copyright 2015 PingCAP, Inc.
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

package ranger

import (
	"cmp"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// RangeType is alias for int.
type RangeType int

// RangeType constants.
const (
	IntRangeType RangeType = iota
	ColumnRangeType
	IndexRangeType
)

// Point is the end point of range interval.
type point struct {
	value types.Datum
	excl  bool // exclude
	start bool
}

func (rp *point) String() string {
	val := rp.value.GetValue()
	if rp.value.Kind() == types.KindMinNotNull {
		val = "-inf"
	} else if rp.value.Kind() == types.KindMaxValue {
		val = "+inf"
	}
	if rp.start {
		symbol := "["
		if rp.excl {
			symbol = "("
		}
		return fmt.Sprintf("%s%v", symbol, val)
	}
	symbol := "]"
	if rp.excl {
		symbol = ")"
	}
	return fmt.Sprintf("%v%s", val, symbol)
}

func (rp *point) Clone(value types.Datum) *point {
	return &point{
		value: value,
		excl:  rp.excl,
		start: rp.start,
	}
}

type pointSorter struct {
	err      error
	collator collate.Collator
	tc       types.Context
	points   []*point
}

func (r *pointSorter) Len() int {
	return len(r.points)
}

func (r *pointSorter) Less(i, j int) bool {
	a := r.points[i]
	b := r.points[j]
	less, err := rangePointLess(r.tc, a, b, r.collator)
	if err != nil {
		r.err = err
	}
	return less
}

func rangePointLess(tc types.Context, a, b *point, collator collate.Collator) (bool, error) {
	if a.value.Kind() == types.KindMysqlEnum && b.value.Kind() == types.KindMysqlEnum {
		return rangePointEnumLess(a, b)
	}
	cmp, err := a.value.Compare(tc, &b.value, collator)
	if cmp != 0 {
		return cmp < 0, nil
	}
	return rangePointEqualValueLess(a, b), errors.Trace(err)
}

func rangePointEnumLess(a, b *point) (bool, error) {
	cmp := cmp.Compare(a.value.GetInt64(), b.value.GetInt64())
	if cmp != 0 {
		return cmp < 0, nil
	}
	return rangePointEqualValueLess(a, b), nil
}

func rangePointEqualValueLess(a, b *point) bool {
	if a.start && b.start {
		return !a.excl && b.excl
	} else if a.start {
		return !a.excl && !b.excl
	} else if b.start {
		return a.excl || b.excl
	}
	return a.excl && !b.excl
}

func pointsConvertToSortKey(sctx *rangerctx.RangerContext, inputPs []*point, newTp *types.FieldType) ([]*point, error) {
	// Only handle normal string type here.
	// Currently, set won't be pushed down and it shouldn't reach here in theory.
	// For enum, we have separate logic for it, like handleEnumFromBinOp(). For now, it only supports point range,
	// intervals are not supported. So we also don't need to handle enum here.
	if newTp.EvalType() != types.ETString ||
		newTp.GetType() == mysql.TypeEnum ||
		newTp.GetType() == mysql.TypeSet {
		return inputPs, nil
	}
	ps := make([]*point, 0, len(inputPs))
	for _, p := range inputPs {
		np, err := pointConvertToSortKey(sctx, p, newTp, true)
		if err != nil {
			return nil, err
		}
		ps = append(ps, np)
	}
	return ps, nil
}

func pointConvertToSortKey(
	sctx *rangerctx.RangerContext,
	inputP *point,
	newTp *types.FieldType,
	trimTrailingSpace bool,
) (*point, error) {
	p, err := convertPoint(sctx, inputP, newTp)
	if err != nil {
		return nil, err
	}
	if p.value.Kind() != types.KindString || newTp.GetCollate() == charset.CollationBin || !collate.NewCollationEnabled() {
		return p, nil
	}
	sortKey := p.value.GetBytes()
	if !trimTrailingSpace {
		sortKey = collate.GetCollator(newTp.GetCollate()).KeyWithoutTrimRightSpace(string(hack.String(sortKey)))
	} else {
		sortKey = collate.GetCollator(newTp.GetCollate()).Key(string(hack.String(sortKey)))
	}

	return &point{value: types.NewBytesDatum(sortKey), excl: p.excl, start: p.start}, nil
}

func (r *pointSorter) Swap(i, j int) {
	r.points[i], r.points[j] = r.points[j], r.points[i]
}

/*
 * If use []point, fullRange will be copied when used.
 * So for keep this behaver, getFullRange function is introduced.
 */
func getFullRange() []*point {
	return []*point{
		{start: true},
		{value: types.MaxValueDatum()},
	}
}

func getNotNullFullRange() []*point {
	return []*point{
		{value: types.MinNotNullDatum(), start: true},
		{value: types.MaxValueDatum()},
	}
}

// FullIntRange is used for table range. Since table range cannot accept MaxValueDatum as the max value.
// So we need to set it to MaxInt64.
func FullIntRange(isUnsigned bool) Ranges {
	if isUnsigned {
		return Ranges{{
			LowVal:    []types.Datum{types.NewUintDatum(0)},
			HighVal:   []types.Datum{types.NewUintDatum(math.MaxUint64)},
			Collators: collate.GetBinaryCollatorSlice(1),
		}}
	}
	return Ranges{{
		LowVal:    []types.Datum{types.NewIntDatum(math.MinInt64)},
		HighVal:   []types.Datum{types.NewIntDatum(math.MaxInt64)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}}
}

// FullRange is [null, +∞) for Range.
func FullRange() Ranges {
	return Ranges{{
		LowVal:    []types.Datum{{}},
		HighVal:   []types.Datum{types.MaxValueDatum()},
		Collators: collate.GetBinaryCollatorSlice(1),
	}}
}

// FullNotNullRange is (-∞, +∞) for Range.
func FullNotNullRange() Ranges {
	return Ranges{{
		LowVal:    []types.Datum{types.MinNotNullDatum()},
		HighVal:   []types.Datum{types.MaxValueDatum()},
		Collators: collate.GetBinaryCollatorSlice(1),
	}}
}

// NullRange is [null, null] for Range.
func NullRange() Ranges {
	return Ranges{{
		LowVal:    []types.Datum{{}},
		HighVal:   []types.Datum{{}},
		Collators: collate.GetBinaryCollatorSlice(1),
	}}
}

// builder is the range builder struct.
type builder struct {
	err  error
	sctx *rangerctx.RangerContext
}

// build converts Expression on one column into point, which can be further built into Range.
// If the input prefixLen is not types.UnspecifiedLength, it means it's for a prefix column in a prefix index. In such
// cases, we should cut the prefix and adjust the exclusiveness. Ref: cutPrefixForPoints().
// convertToSortKey indicates whether the string values should be converted to sort key.
// Converting to sort key can make `like` function be built into Range for new collation column. But we can't restore
// the original value from the sort key, so the usage of the result may be limited, like when you need to restore the
// result points back to Expression.
func (r *builder) build(
	expr expression.Expression,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) []*point {
	switch x := expr.(type) {
	case *expression.Column:
		return r.buildFromColumn()
	case *expression.ScalarFunction:
		return r.buildFromScalarFunc(x, newTp, prefixLen, convertToSortKey)
	case *expression.Constant:
		return r.buildFromConstant(x)
	}

	return getFullRange()
}

func (r *builder) buildFromConstant(expr *expression.Constant) []*point {
	dt, err := expr.Eval(r.sctx.ExprCtx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		r.err = err
		return nil
	}
	if dt.IsNull() {
		return nil
	}

	tc := r.sctx.TypeCtx
	val, err := dt.ToBool(tc)
	if err != nil {
		r.err = err
		return nil
	}

	if val == 0 {
		return nil
	}
	return getFullRange()
}

func (*builder) buildFromColumn() []*point {
	// column name expression is equivalent to column name is true.
	startPoint1 := &point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := &point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := &point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := &point{value: types.MaxValueDatum()}
	return []*point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFromBinOp(
	expr *expression.ScalarFunction,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) []*point {
	// This has been checked that the binary operation is comparison operation, and one of
	// the operand is column name expression.
	var (
		op    string
		value types.Datum
		err   error
		ft    *types.FieldType
	)

	tc := r.sctx.TypeCtx
	// refineValueAndOp refines the constant datum and operator:
	// 1. for string type since we may eval the constant to another collation instead of its own collation.
	// 2. for year type since 2-digit year value need adjustment, see https://dev.mysql.com/doc/refman/5.6/en/year.html
	refineValueAndOp := func(col *expression.Column, value *types.Datum, op *string) (err error) {
		if col.RetType.EvalType() == types.ETString && (value.Kind() == types.KindString || value.Kind() == types.KindBinaryLiteral) {
			value.SetString(value.GetString(), col.RetType.GetCollate())
		}
		// If nulleq with null value, values.ToInt64 will return err
		if col.GetType(r.sctx.ExprCtx.GetEvalCtx()).GetType() == mysql.TypeYear && !value.IsNull() {
			// Convert the out-of-range uint number to int and then let the following logic can handle it correctly.
			// Since the max value of year is 2155, `col op MaxUint` should have the same result with `col op MaxInt`.
			if value.Kind() == types.KindUint64 && value.GetUint64() > math.MaxInt64 {
				value.SetInt64(math.MaxInt64)
			}

			// If the original value is adjusted, we need to change the condition.
			// For example, col < 2156. Since the max year is 2155, 2156 is changed to 2155.
			// col < 2155 is wrong. It should be col <= 2155.
			preValue, err1 := value.ToInt64(tc)
			if err1 != nil {
				return err1
			}
			*value, err = value.ConvertToMysqlYear(tc, col.RetType)
			if errors.ErrorEqual(err, types.ErrWarnDataOutOfRange) {
				// Keep err for EQ and NE.
				switch *op {
				case ast.GT:
					if value.GetInt64() > preValue {
						*op = ast.GE
					}
					err = nil
				case ast.LT:
					if value.GetInt64() < preValue {
						*op = ast.LE
					}
					err = nil
				case ast.GE, ast.LE:
					err = nil
				}
			}
		}
		return
	}
	var col *expression.Column
	var ok bool
	if col, ok = expr.GetArgs()[0].(*expression.Column); ok {
		ft = col.RetType
		value, err = expr.GetArgs()[1].Eval(r.sctx.ExprCtx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil
		}
		op = expr.FuncName.L
	} else {
		col, ok = expr.GetArgs()[1].(*expression.Column)
		if !ok {
			return nil
		}
		ft = col.RetType
		value, err = expr.GetArgs()[0].Eval(r.sctx.ExprCtx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil
		}
		switch expr.FuncName.L {
		case ast.GE:
			op = ast.LE
		case ast.GT:
			op = ast.LT
		case ast.LT:
			op = ast.GT
		case ast.LE:
			op = ast.GE
		default:
			op = expr.FuncName.L
		}
	}
	if op != ast.NullEQ && value.IsNull() {
		return nil
	}
	err = refineValueAndOp(col, &value, &op)
	if err != nil {
		if op == ast.NE {
			// col != an impossible value (not valid year)
			return getNotNullFullRange()
		}
		// col = an impossible value (not valid year)
		return nil
	}

	value, op, isValidRange := handleUnsignedCol(ft, value, op)
	if !isValidRange {
		return nil
	}

	value, op, isValidRange = handleBoundCol(ft, value, op)
	if !isValidRange {
		return nil
	}

	if ft.GetType() == mysql.TypeEnum && ft.EvalType() == types.ETString {
		return handleEnumFromBinOp(tc, ft, value, op)
	}

	var res []*point
	switch op {
	case ast.NullEQ:
		if value.IsNull() {
			res = []*point{{start: true}, {}} // [null, null]
			break
		}
		fallthrough
	case ast.EQ:
		startPoint := &point{value: value, start: true}
		endPoint := &point{value: value}
		res = []*point{startPoint, endPoint}
	case ast.NE:
		startPoint1 := &point{value: types.MinNotNullDatum(), start: true}
		endPoint1 := &point{value: value, excl: true}
		startPoint2 := &point{value: value, start: true, excl: true}
		endPoint2 := &point{value: types.MaxValueDatum()}
		res = []*point{startPoint1, endPoint1, startPoint2, endPoint2}
	case ast.LT:
		startPoint := &point{value: types.MinNotNullDatum(), start: true}
		endPoint := &point{value: value, excl: true}
		res = []*point{startPoint, endPoint}
	case ast.LE:
		startPoint := &point{value: types.MinNotNullDatum(), start: true}
		endPoint := &point{value: value}
		res = []*point{startPoint, endPoint}
	case ast.GT:
		startPoint := &point{value: value, start: true, excl: true}
		endPoint := &point{value: types.MaxValueDatum()}
		res = []*point{startPoint, endPoint}
	case ast.GE:
		startPoint := &point{value: value, start: true}
		endPoint := &point{value: types.MaxValueDatum()}
		res = []*point{startPoint, endPoint}
	}
	cutPrefixForPoints(res, prefixLen, ft)
	if convertToSortKey {
		res, err = pointsConvertToSortKey(r.sctx, res, newTp)
		if err != nil {
			r.err = err
			return getFullRange()
		}
	}
	return res
}

// handleUnsignedCol handles the case when unsigned column meets negative value.
// The three returned values are: fixed constant value, fixed operator, and a boolean
// which indicates whether the range is valid or not.
func handleUnsignedCol(ft *types.FieldType, val types.Datum, op string) (types.Datum, string, bool) {
	isUnsigned := mysql.HasUnsignedFlag(ft.GetFlag())
	isNegative := (val.Kind() == types.KindInt64 && val.GetInt64() < 0) ||
		(val.Kind() == types.KindFloat32 && val.GetFloat32() < 0) ||
		(val.Kind() == types.KindFloat64 && val.GetFloat64() < 0) ||
		(val.Kind() == types.KindMysqlDecimal && val.GetMysqlDecimal().IsNegative())

	if !isUnsigned || !isNegative {
		return val, op, true
	}

	// If the operator is GT, GE or NE, the range should be [0, +inf].
	// Otherwise the value is out of valid range.
	if op == ast.GT || op == ast.GE || op == ast.NE {
		op = ast.GE
		switch val.Kind() {
		case types.KindInt64:
			val.SetUint64(0)
		case types.KindFloat32:
			val.SetFloat32(0)
		case types.KindFloat64:
			val.SetFloat64(0)
		case types.KindMysqlDecimal:
			val.SetMysqlDecimal(new(types.MyDecimal))
		}
		return val, op, true
	}

	return val, op, false
}

// handleBoundCol handles the case when column meets overflow value.
// The three returned values are: fixed constant value, fixed operator, and a boolean
// which indicates whether the range is valid or not.
func handleBoundCol(ft *types.FieldType, val types.Datum, op string) (types.Datum, string, bool) {
	isUnsigned := mysql.HasUnsignedFlag(ft.GetFlag())
	isNegative := val.Kind() == types.KindInt64 && val.GetInt64() < 0
	if isUnsigned {
		return val, op, true
	}

	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if !isNegative && val.GetUint64() > math.MaxInt64 {
			switch op {
			case ast.GT, ast.GE:
				return val, op, false
			case ast.NE, ast.LE, ast.LT:
				op = ast.LE
				val = types.NewIntDatum(math.MaxInt64)
			}
		}
	case mysql.TypeFloat:
		if val.GetFloat64() > math.MaxFloat32 {
			switch op {
			case ast.GT, ast.GE:
				return val, op, false
			case ast.NE, ast.LE, ast.LT:
				op = ast.LE
				val = types.NewFloat32Datum(math.MaxFloat32)
			}
		} else if val.GetFloat64() < -math.MaxFloat32 {
			switch op {
			case ast.LE, ast.LT:
				return val, op, false
			case ast.GT, ast.GE, ast.NE:
				op = ast.GE
				val = types.NewFloat32Datum(-math.MaxFloat32)
			}
		}
	}

	return val, op, true
}

func handleEnumFromBinOp(tc types.Context, ft *types.FieldType, val types.Datum, op string) []*point {
	res := make([]*point, 0, len(ft.GetElems())*2)
	appendPointFunc := func(d types.Datum) {
		res = append(res, &point{value: d, excl: false, start: true})
		res = append(res, &point{value: d, excl: false, start: false})
	}

	if op == ast.NullEQ && val.IsNull() {
		res = append(res, &point{start: true}, &point{}) // null point
	}

	tmpEnum := types.Enum{}
	for i := 0; i <= len(ft.GetElems()); i++ {
		if i == 0 {
			tmpEnum = types.Enum{}
		} else {
			tmpEnum.Name = ft.GetElems()[i-1]
			tmpEnum.Value = uint64(i)
		}

		d := types.NewCollateMysqlEnumDatum(tmpEnum, ft.GetCollate())
		if v, err := d.Compare(tc, &val, collate.GetCollator(ft.GetCollate())); err == nil {
			switch op {
			case ast.LT:
				if v < 0 {
					appendPointFunc(d)
				}
			case ast.LE:
				if v <= 0 {
					appendPointFunc(d)
				}
			case ast.GT:
				if v > 0 {
					appendPointFunc(d)
				}
			case ast.GE:
				if v >= 0 {
					appendPointFunc(d)
				}
			case ast.EQ, ast.NullEQ:
				if v == 0 {
					appendPointFunc(d)
				}
			case ast.NE:
				if v != 0 {
					appendPointFunc(d)
				}
			}
		}
	}
	return res
}

func (*builder) buildFromIsTrue(_ *expression.ScalarFunction, isNot int, keepNull bool) []*point {
	if isNot == 1 {
		if keepNull {
			// Range is {[0, 0]}
			startPoint := &point{start: true}
			startPoint.value.SetInt64(0)
			endPoint := &point{}
			endPoint.value.SetInt64(0)
			return []*point{startPoint, endPoint}
		}
		// NOT TRUE range is {[null null] [0, 0]}
		startPoint1 := &point{start: true}
		endPoint1 := &point{}
		startPoint2 := &point{start: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := &point{}
		endPoint2.value.SetInt64(0)
		return []*point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// TRUE range is {[-inf 0) (0 +inf]}
	startPoint1 := &point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := &point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := &point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := &point{value: types.MaxValueDatum()}
	return []*point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (*builder) buildFromIsFalse(_ *expression.ScalarFunction, isNot int) []*point {
	if isNot == 1 {
		// NOT FALSE range is {[-inf, 0), (0, +inf], [null, null]}
		startPoint1 := &point{start: true}
		endPoint1 := &point{excl: true}
		endPoint1.value.SetInt64(0)
		startPoint2 := &point{start: true, excl: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := &point{value: types.MaxValueDatum()}
		return []*point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// FALSE range is {[0, 0]}
	startPoint := &point{start: true}
	startPoint.value.SetInt64(0)
	endPoint := &point{}
	endPoint.value.SetInt64(0)
	return []*point{startPoint, endPoint}
}

func (r *builder) buildFromIn(
	expr *expression.ScalarFunction,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) ([]*point, bool) {
	list := expr.GetArgs()[1:]
	rangePoints := make([]*point, 0, len(list)*2)
	hasNull := false
	ft := expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx())
	colCollate := ft.GetCollate()
	tc := r.sctx.TypeCtx
	evalCtx := r.sctx.ExprCtx.GetEvalCtx()
	for _, e := range list {
		v, ok := e.(*expression.Constant)
		if !ok {
			r.err = plannererrors.ErrUnsupportedType.GenWithStack("expr:%v is not constant", e)
			return getFullRange(), hasNull
		}
		dt, err := v.Eval(evalCtx, chunk.Row{})
		if err != nil {
			r.err = plannererrors.ErrUnsupportedType.GenWithStack("expr:%v is not evaluated", e)
			return getFullRange(), hasNull
		}
		if dt.IsNull() {
			hasNull = true
			continue
		}
		if expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()).GetType() == mysql.TypeEnum {
			switch dt.Kind() {
			case types.KindString, types.KindBytes, types.KindBinaryLiteral:
				// Can't use ConvertTo directly, since we shouldn't convert numerical string to Enum in select stmt.
				targetType := expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx())
				enum, parseErr := types.ParseEnumName(targetType.GetElems(), dt.GetString(), targetType.GetCollate())
				if parseErr == nil {
					dt.SetMysqlEnum(enum, targetType.GetCollate())
				} else {
					err = parseErr
				}
			default:
				dt, err = dt.ConvertTo(tc, expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()))
			}

			if err != nil {
				// in (..., an impossible value (not valid enum), ...), the range is empty, so skip it.
				continue
			}
		}
		if expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()).GetType() == mysql.TypeYear {
			dt, err = dt.ConvertToMysqlYear(tc, expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()))
			if err != nil {
				// in (..., an impossible value (not valid year), ...), the range is empty, so skip it.
				continue
			}
		}
		if expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()).EvalType() == types.ETString && (dt.Kind() == types.KindString || dt.Kind() == types.KindBinaryLiteral) {
			dt.SetString(dt.GetString(), expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()).GetCollate()) // refine the string like what we did in builder.buildFromBinOp
		}
		var startValue, endValue types.Datum
		dt.Copy(&startValue)
		dt.Copy(&endValue)
		startPoint := &point{value: startValue, start: true}
		endPoint := &point{value: endValue}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	sorter := pointSorter{points: rangePoints, tc: tc, collator: collate.GetCollator(colCollate)}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
	}
	// check and remove duplicates
	curPos, frontPos := 0, 0
	for frontPos < len(rangePoints) {
		if rangePoints[curPos].start == rangePoints[frontPos].start {
			frontPos++
		} else {
			curPos++
			rangePoints[curPos] = rangePoints[frontPos]
			frontPos++
		}
	}
	if curPos > 0 {
		curPos++
	}
	rangePoints = rangePoints[:curPos]
	cutPrefixForPoints(rangePoints, prefixLen, ft)
	var err error
	if convertToSortKey {
		rangePoints, err = pointsConvertToSortKey(r.sctx, rangePoints, newTp)
		if err != nil {
			r.err = err
			return getFullRange(), false
		}
	}
	return rangePoints, hasNull
}

func (r *builder) newBuildFromPatternLike(
	expr *expression.ScalarFunction,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) []*point {
	_, collation := expr.CharsetAndCollation()
	if !collate.CompatibleCollate(expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()).GetCollate(), collation) {
		return getFullRange()
	}
	pdt, err := expr.GetArgs()[1].(*expression.Constant).Eval(r.sctx.ExprCtx.GetEvalCtx(), chunk.Row{})
	tpOfPattern := expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx())
	if err != nil {
		r.err = errors.Trace(err)
		return getFullRange()
	}
	pattern, err := pdt.ToString()
	if err != nil {
		r.err = errors.Trace(err)
		return getFullRange()
	}
	// non-exceptional return case 1: empty pattern
	if pattern == "" {
		startPoint := &point{value: types.NewStringDatum(""), start: true}
		endPoint := &point{value: types.NewStringDatum("")}
		res := []*point{startPoint, endPoint}
		if convertToSortKey {
			res, err = pointsConvertToSortKey(r.sctx, res, newTp)
			if err != nil {
				r.err = err
				return getFullRange()
			}
		}
		return res
	}
	lowValue := make([]byte, 0, len(pattern))
	edt, err := expr.GetArgs()[2].(*expression.Constant).Eval(r.sctx.ExprCtx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		r.err = errors.Trace(err)
		return getFullRange()
	}
	escape := byte(edt.GetInt64())
	var exclude bool
	isExactMatch := true
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == escape {
			i++
			if i < len(pattern) {
				lowValue = append(lowValue, pattern[i])
			} else {
				lowValue = append(lowValue, escape)
			}
			continue
		}
		if pattern[i] == '%' {
			// Get the prefix.
			isExactMatch = false
			break
		} else if pattern[i] == '_' {
			// Get the prefix, but exclude the prefix.
			// e.g., "abc_x", the start point excludes "abc" because the string length is more than 3.
			//
			// However, like the similar check in (*conditionChecker).checkLikeFunc(), in tidb's implementation, for
			// PAD SPACE collations, the trailing spaces are removed in the index key. So we are unable to distinguish
			// 'xxx' from 'xxx   ' by a single index range scan. If we exclude the start point for PAD SPACE collation,
			// we will actually miss 'xxx   ', which will cause wrong results.
			if !isPadSpaceCollation(collation) {
				exclude = true
			}
			isExactMatch = false
			break
		}
		lowValue = append(lowValue, pattern[i])
	}
	// non-exceptional return case 2: no characters before the wildcard
	if len(lowValue) == 0 {
		return []*point{{value: types.MinNotNullDatum(), start: true}, {value: types.MaxValueDatum()}}
	}
	// non-exceptional return case 3: pattern contains valid characters and doesn't contain the wildcard
	if isExactMatch {
		val := types.NewCollationStringDatum(string(lowValue), tpOfPattern.GetCollate())
		startPoint := &point{value: val, start: true}
		endPoint := &point{value: val}
		res := []*point{startPoint, endPoint}
		cutPrefixForPoints(res, prefixLen, tpOfPattern)
		if convertToSortKey {
			res, err = pointsConvertToSortKey(r.sctx, res, newTp)
			if err != nil {
				r.err = err
				return getFullRange()
			}
		}
		return res
	}

	// non-exceptional return case 4: pattern contains valid characters and contains the wildcard

	// non-exceptional return case 4-1
	// If it's not a _bin or binary collation, and we don't convert the value to the sort key, we can't build
	// a range for the wildcard.
	if !convertToSortKey &&
		!collate.IsBinCollation(tpOfPattern.GetCollate()) {
		return []*point{{value: types.MinNotNullDatum(), start: true}, {value: types.MaxValueDatum()}}
	}

	// non-exceptional return case 4-2: build a range for the wildcard
	// the end_key is sortKey(start_value) + 1
	originalStartPoint := &point{start: true, excl: exclude}
	originalStartPoint.value.SetBytesAsString(lowValue, tpOfPattern.GetCollate(), uint32(tpOfPattern.GetFlen()))
	cutPrefixForPoints([]*point{originalStartPoint}, prefixLen, tpOfPattern)

	// If we don't trim the trailing spaces, which means using KeyWithoutTrimRightSpace() instead of Key(), we can build
	// a smaller range for better performance, e.g., LIKE '  %'.
	// However, if it's a PAD SPACE collation, we must trim the trailing spaces for the start point to ensure the correctness.
	// Because the trailing spaces are trimmed in the stored index key. For example, for LIKE 'abc  %' on utf8mb4_bin
	// column, the start key should be 'abd' instead of 'abc ', but the end key can be 'abc!'. ( ' ' is 32 and '!' is 33
	// in ASCII)
	shouldTrimTrailingSpace := isPadSpaceCollation(collation)
	startPoint, err := pointConvertToSortKey(r.sctx, originalStartPoint, newTp, shouldTrimTrailingSpace)
	if err != nil {
		r.err = errors.Trace(err)
		return getFullRange()
	}
	sortKeyPointWithoutTrim, err := pointConvertToSortKey(r.sctx, originalStartPoint, newTp, false)
	if err != nil {
		r.err = errors.Trace(err)
		return getFullRange()
	}
	sortKeyWithoutTrim := append([]byte{}, sortKeyPointWithoutTrim.value.GetBytes()...)
	endPoint := &point{value: types.MaxValueDatum(), excl: true}
	for i := len(sortKeyWithoutTrim) - 1; i >= 0; i-- {
		// Make the end point value more than the start point value,
		// and the length of the end point value is the same as the length of the start point value.
		// e.g., the start point value is "abc", so the end point value is "abd".
		sortKeyWithoutTrim[i]++
		if sortKeyWithoutTrim[i] != 0 {
			endPoint.value.SetBytes(sortKeyWithoutTrim)
			break
		}
		// If sortKeyWithoutTrim[i] is 255 and sortKeyWithoutTrim[i]++ is 0, then the end point value is max value.
		if i == 0 {
			endPoint.value = types.MaxValueDatum()
		}
	}
	return []*point{startPoint, endPoint}
}

// isPadSpaceCollation returns whether the collation is a PAD SPACE collation.
// Since all collations, except for binary, implemented in tidb are PAD SPACE collations for now, we use a simple
// collation != binary check here. We may also move it to collation related packages when NO PAD collations are
// implemented in the future.
func isPadSpaceCollation(collation string) bool {
	return collation != charset.CollationBin
}

func (r *builder) buildFromNot(
	expr *expression.ScalarFunction,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) []*point {
	switch n := expr.FuncName.L; n {
	case ast.IsTruthWithoutNull:
		return r.buildFromIsTrue(expr, 1, false)
	case ast.IsTruthWithNull:
		return r.buildFromIsTrue(expr, 1, true)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 1)
	case ast.In:
		var (
			isUnsignedIntCol bool
			nonNegativePos   int
		)
		// Note that we must handle the cutting prefix and converting to sort key in buildFromNot, because if we cut the
		// prefix inside buildFromIn(), the inversion logic here would make an incomplete and wrong range.
		// For example, for index col(1), col NOT IN ('aaa', 'bbb'), if we cut the prefix in buildFromIn(), we would get
		// ['a', 'a'], ['b', 'b'] from there. Then after in this function we would get ['' 'a'), ('a', 'b'), ('b', +inf]
		// as the result. This is wrong because data like 'ab' would be missed. Actually we are unable to build a range
		// for this case.
		// So we must cut the prefix in this function, therefore converting to sort key must also be done here.
		rangePoints, hasNull := r.buildFromIn(expr, newTp, types.UnspecifiedLength, false)
		if hasNull {
			return nil
		}
		if x, ok := expr.GetArgs()[0].(*expression.Column); ok {
			isUnsignedIntCol = mysql.HasUnsignedFlag(x.RetType.GetFlag()) && mysql.IsIntegerType(x.RetType.GetType())
		}
		// negative ranges can be directly ignored for unsigned int columns.
		if isUnsignedIntCol {
			for nonNegativePos = 0; nonNegativePos < len(rangePoints); nonNegativePos += 2 {
				if rangePoints[nonNegativePos].value.Kind() == types.KindUint64 || rangePoints[nonNegativePos].value.GetInt64() >= 0 {
					break
				}
			}
			rangePoints = rangePoints[nonNegativePos:]
		}
		retRangePoints := make([]*point, 0, 2+len(rangePoints))
		previousValue := types.Datum{}
		for i := 0; i < len(rangePoints); i += 2 {
			retRangePoints = append(retRangePoints, &point{value: previousValue, start: true, excl: true})
			retRangePoints = append(retRangePoints, &point{value: rangePoints[i].value, excl: true})
			previousValue = rangePoints[i].value
		}
		// Append the interval (last element, max value].
		retRangePoints = append(retRangePoints, &point{value: previousValue, start: true, excl: true})
		retRangePoints = append(retRangePoints, &point{value: types.MaxValueDatum()})
		cutPrefixForPoints(retRangePoints, prefixLen, expr.GetArgs()[0].GetType(r.sctx.ExprCtx.GetEvalCtx()))
		if convertToSortKey {
			var err error
			retRangePoints, err = pointsConvertToSortKey(r.sctx, retRangePoints, newTp)
			if err != nil {
				r.err = err
				return getFullRange()
			}
		}
		return retRangePoints
	case ast.Like:
		// Pattern not like is not supported.
		r.err = plannererrors.ErrUnsupportedType.GenWithStack("NOT LIKE is not supported.")
		return getFullRange()
	case ast.IsNull:
		startPoint := &point{value: types.MinNotNullDatum(), start: true}
		endPoint := &point{value: types.MaxValueDatum()}
		return []*point{startPoint, endPoint}
	}
	// TODO: currently we don't handle ast.LogicAnd, ast.LogicOr, ast.GT, ast.LT and so on. Most of those cases are eliminated
	// by PushDownNot but they may happen. For now, we return full range for those unhandled cases in order to keep correctness.
	// Later we need to cover those cases and set r.err when meeting some unexpected case.
	return getFullRange()
}

func (r *builder) buildFromScalarFunc(
	expr *expression.ScalarFunction,
	newTp *types.FieldType,
	prefixLen int,
	convertToSortKey bool,
) []*point {
	switch op := expr.FuncName.L; op {
	case ast.GE, ast.GT, ast.LT, ast.LE, ast.EQ, ast.NE, ast.NullEQ:
		return r.buildFromBinOp(expr, newTp, prefixLen, convertToSortKey)
	case ast.LogicAnd:
		collator := collate.GetCollator(newTp.GetCollate())
		if convertToSortKey {
			collator = collate.GetCollator(charset.CollationBin)
		}
		return r.intersection(r.build(expr.GetArgs()[0], newTp, prefixLen, convertToSortKey), r.build(expr.GetArgs()[1], newTp, prefixLen, convertToSortKey), collator)
	case ast.LogicOr:
		collator := collate.GetCollator(newTp.GetCollate())
		if convertToSortKey {
			collator = collate.GetCollator(charset.CollationBin)
		}
		return r.union(r.build(expr.GetArgs()[0], newTp, prefixLen, convertToSortKey), r.build(expr.GetArgs()[1], newTp, prefixLen, convertToSortKey), collator)
	case ast.IsTruthWithoutNull:
		return r.buildFromIsTrue(expr, 0, false)
	case ast.IsTruthWithNull:
		return r.buildFromIsTrue(expr, 0, true)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 0)
	case ast.In:
		retPoints, _ := r.buildFromIn(expr, newTp, prefixLen, convertToSortKey)
		return retPoints
	case ast.Like:
		return r.newBuildFromPatternLike(expr, newTp, prefixLen, convertToSortKey)
	case ast.IsNull:
		startPoint := &point{start: true}
		endPoint := &point{}
		return []*point{startPoint, endPoint}
	case ast.UnaryNot:
		return r.buildFromNot(expr.GetArgs()[0].(*expression.ScalarFunction), newTp, prefixLen, convertToSortKey)
	}

	return nil
}

// We need an input collator because our (*Datum).Compare(), which is used in this method, needs an explicit collator
// input to handle comparison for string and bytes.
// Note that if the points are converted to sort key, the collator should be set to charset.CollationBin.
func (r *builder) intersection(a, b []*point, collator collate.Collator) []*point {
	return r.merge(a, b, false, collator)
}

// We need an input collator because our (*Datum).Compare(), which is used in this method, needs an explicit collator
// input to handle comparison for string and bytes.
// Note that if the points are converted to sort key, the collator should be set to charset.CollationBin.
func (r *builder) union(a, b []*point, collator collate.Collator) []*point {
	return r.merge(a, b, true, collator)
}

func (r *builder) mergeSorted(a, b []*point, collator collate.Collator) []*point {
	ret := make([]*point, 0, len(a)+len(b))
	i, j := 0, 0
	tc := r.sctx.TypeCtx
	for i < len(a) && j < len(b) {
		less, err := rangePointLess(tc, a[i], b[j], collator)
		if err != nil {
			r.err = err
			return nil
		}
		if less {
			ret = append(ret, a[i])
			i++
		} else {
			ret = append(ret, b[j])
			j++
		}
	}
	if i < len(a) {
		ret = append(ret, a[i:]...)
	} else if j < len(b) {
		ret = append(ret, b[j:]...)
	}
	return ret
}

func (r *builder) merge(a, b []*point, union bool, collator collate.Collator) []*point {
	mergedPoints := r.mergeSorted(a, b, collator)
	if r.err != nil {
		return nil
	}

	var (
		inRangeCount         int
		requiredInRangeCount int
	)
	if union {
		requiredInRangeCount = 1
	} else {
		requiredInRangeCount = 2
	}
	curTail := 0
	for _, val := range mergedPoints {
		if val.start {
			inRangeCount++
			if inRangeCount == requiredInRangeCount {
				// Just reached the required in range count, a new range started.
				mergedPoints[curTail] = val
				curTail++
			}
		} else {
			if inRangeCount == requiredInRangeCount {
				// Just about to leave the required in range count, the range is ended.
				mergedPoints[curTail] = val
				curTail++
			}
			inRangeCount--
		}
	}
	return mergedPoints[:curTail]
}
