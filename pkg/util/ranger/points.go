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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
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

func rangePointCmp(tc types.Context, a, b *point, collator collate.Collator) (int, error) {
	if a.value.Kind() == types.KindMysqlEnum && b.value.Kind() == types.KindMysqlEnum {
		return rangePointEnumCmp(a, b)
	}
	cmp, err := a.value.Compare(tc, &b.value, collator)
	if cmp != 0 {
		return cmp, nil
	}
	return rangePointEqualValueCmp(a, b), errors.Trace(err)
}

func rangePointEnumCmp(a, b *point) (int, error) {
	cmp := cmp.Compare(a.value.GetInt64(), b.value.GetInt64())
	if cmp != 0 {
		return cmp, nil
	}
	return rangePointEqualValueCmp(a, b), nil
}

func rangePointEqualValueCmp(a, b *point) int {
	var result bool
	if a.start && b.start {
		result = !a.excl && b.excl
	} else if a.start {
		result = !a.excl && !b.excl
	} else if b.start {
		result = a.excl || b.excl
	} else {
		result = a.excl && !b.excl
	}
	if result {
		return -1
	}
	return 0
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
		less, err := rangePointCmp(tc, a[i], b[j], collator)
		if err != nil {
			r.err = err
			return nil
		}
		if less < 0 {
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
