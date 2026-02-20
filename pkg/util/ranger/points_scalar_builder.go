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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

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
			r.err = plannererrors.ErrUnsupportedType.GenWithStack("expr:%v is not constant", e.StringWithCtx(evalCtx, errors.RedactLogDisable))
			return getFullRange(), hasNull
		}
		dt, err := v.Eval(evalCtx, chunk.Row{})
		if err != nil {
			r.err = plannererrors.ErrUnsupportedType.GenWithStack("expr:%v is not evaluated", e.StringWithCtx(evalCtx, errors.RedactLogDisable))
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
	collator := collate.GetCollator(colCollate)
	slices.SortFunc(rangePoints, func(a, b *point) (cmpare int) {
		cmpare, r.err = rangePointCmp(tc, a, b, collator)
		return cmpare
	})
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
			if !collate.IsPadSpaceCollation(collation) {
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
	shouldTrimTrailingSpace := collate.IsPadSpaceCollation(collation)
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
	sortKeyWithoutTrim := slices.Clone(sortKeyPointWithoutTrim.value.GetBytes())
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
