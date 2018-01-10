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

package ranger

import (
	"bytes"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// points2NewRanges build index ranges from range points.
// Only one column is built there. If there're multiple columns, use appendPoints2NewRanges.
func points2NewRanges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*NewRange, error) {
	indexRanges := make([]*NewRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sc, rangePoints[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sc, rangePoints[i+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := rangePointLess(sc, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		ir := &NewRange{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		indexRanges = append(indexRanges, ir)
	}
	return indexRanges, nil
}

func convertPoint(sc *stmtctx.StatementContext, point point, tp *types.FieldType) (point, error) {
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point, nil
	}
	casted, err := point.value.ConvertTo(sc, tp)
	if err != nil {
		return point, errors.Trace(err)
	}
	valCmpCasted, err := point.value.CompareDatum(sc, &casted)
	if err != nil {
		return point, errors.Trace(err)
	}
	point.value = casted
	if valCmpCasted == 0 {
		return point, nil
	}
	if point.start {
		if point.excl {
			if valCmpCasted < 0 {
				// e.g. "a > 1.9" convert to "a >= 2".
				point.excl = false
			}
		} else {
			if valCmpCasted > 0 {
				// e.g. "a >= 1.1 convert to "a > 1"
				point.excl = true
			}
		}
	} else {
		if point.excl {
			if valCmpCasted > 0 {
				// e.g. "a < 1.1" convert to "a <= 1"
				point.excl = false
			}
		} else {
			if valCmpCasted < 0 {
				// e.g. "a <= 1.9" convert to "a < 2"
				point.excl = true
			}
		}
	}
	return point, nil
}

// appendPoints2NewRanges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func appendPoints2NewRanges(sc *stmtctx.StatementContext, origin []*NewRange, rangePoints []point,
	ft *types.FieldType) ([]*NewRange, error) {
	var newIndexRanges []*NewRange
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(sc) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newRanges, err := appendPoints2IndexRange(sc, oRange, rangePoints, ft)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newIndexRanges = append(newIndexRanges, newRanges...)
		}
	}
	return newIndexRanges, nil
}

func appendPoints2IndexRange(sc *stmtctx.StatementContext, origin *NewRange, rangePoints []point,
	ft *types.FieldType) ([]*NewRange, error) {
	newRanges := make([]*NewRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sc, rangePoints[i], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sc, rangePoints[i+1], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := rangePointLess(sc, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}

		lowVal := make([]types.Datum, len(origin.LowVal)+1)
		copy(lowVal, origin.LowVal)
		lowVal[len(origin.LowVal)] = startPoint.value

		highVal := make([]types.Datum, len(origin.HighVal)+1)
		copy(highVal, origin.HighVal)
		highVal[len(origin.HighVal)] = endPoint.value

		ir := &NewRange{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges, nil
}

// points2TableRanges will construct the range slice with the given range points
func points2TableRanges(sc *stmtctx.StatementContext, rangePoints []point) ([]IntColumnRange, error) {
	tableRanges := make([]IntColumnRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		if startPoint.value.IsNull() || startPoint.value.Kind() == types.KindMinNotNull {
			if startPoint.value.IsNull() {
				startPoint.excl = false
			}
			startPoint.value.SetInt64(math.MinInt64)
		}
		startInt, err := startPoint.value.ToInt64(sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		startDatum := types.NewDatum(startInt)
		cmp, err := startDatum.CompareDatum(sc, &startPoint.value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp < 0 || (cmp == 0 && startPoint.excl) {
			if startInt == math.MaxInt64 {
				continue
			}
			startInt++
		}
		endPoint := rangePoints[i+1]
		if endPoint.value.IsNull() {
			continue
		} else if endPoint.value.Kind() == types.KindMaxValue {
			endPoint.value.SetInt64(math.MaxInt64)
		}
		endInt, err := endPoint.value.ToInt64(sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endDatum := types.NewDatum(endInt)
		cmp, err = endDatum.CompareDatum(sc, &endPoint.value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp > 0 || (cmp == 0 && endPoint.excl) {
			if endInt == math.MinInt64 {
				continue
			}
			endInt--
		}
		if startInt > endInt {
			continue
		}
		tableRanges = append(tableRanges, IntColumnRange{LowVal: startInt, HighVal: endInt})
	}
	return tableRanges, nil
}

// BuildTableRange will build range of pk for PhysicalTableScan
func BuildTableRange(accessConditions []expression.Expression, sc *stmtctx.StatementContext) ([]IntColumnRange, error) {
	if len(accessConditions) == 0 {
		return FullIntRange(), nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range accessConditions {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges, err := points2TableRanges(sc, rangePoints)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

// BuildColumnRange builds the range for sampling histogram to calculate the row count.
func BuildColumnRange(conds []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType) ([]*NewRange, error) {
	if len(conds) == 0 {
		return []*NewRange{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{types.MaxValueDatum()}}}, nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range conds {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges, err := points2NewRanges(sc, rangePoints, tp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

// BuildCNFIndexRange builds the range for index.
func BuildCNFIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, lengths []int,
	accessCondition []expression.Expression) ([]*NewRange, error) {
	rb := builder{sc: sc}
	var (
		ranges       []*NewRange
		eqAndInCount int
		err          error
	)
	for eqAndInCount = 0; eqAndInCount < len(accessCondition) && eqAndInCount < len(cols); eqAndInCount++ {
		if sf, ok := accessCondition[eqAndInCount].(*expression.ScalarFunction); !ok || (sf.FuncName.L != ast.EQ && sf.FuncName.L != ast.In) {
			break
		}
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[eqAndInCount])
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
		if eqAndInCount == 0 {
			ranges, err = points2NewRanges(sc, point, cols[eqAndInCount].RetType)
		} else {
			ranges, err = appendPoints2NewRanges(sc, ranges, point, cols[eqAndInCount].RetType)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	if eqAndInCount == 0 {
		ranges, err = points2NewRanges(sc, rangePoints, cols[0].RetType)
	} else if eqAndInCount < len(accessCondition) {
		ranges, err = appendPoints2NewRanges(sc, ranges, rangePoints, cols[eqAndInCount].RetType)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Take prefix index into consideration.
	if hasPrefix(lengths) {
		fixPrefixColRange(ranges, lengths)
	}

	if len(ranges) > 0 && len(ranges[0].LowVal) < len(cols) {
		for _, ran := range ranges {
			if ran.HighExclude || ran.LowExclude {
				if ran.HighExclude {
					ran.HighVal = append(ran.HighVal, types.NewDatum(nil))
				} else {
					ran.HighVal = append(ran.HighVal, types.MaxValueDatum())
				}
				if ran.LowExclude {
					ran.LowVal = append(ran.LowVal, types.MaxValueDatum())
				} else {
					ran.LowVal = append(ran.LowVal, types.NewDatum(nil))
				}
			}
		}
	}
	return ranges, nil
}

// BuildDNFIndexRange builds the range for index.
func BuildDNFIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, lengths []int,
	accessCondition []expression.Expression) ([]*NewRange, error) {
	sf := accessCondition[0].(*expression.ScalarFunction)
	totalRanges := make([]*NewRange, 0, len(sf.GetArgs()))
	var err error
	for _, arg := range sf.GetArgs() {
		var partRanges []*NewRange
		if sf, ok := arg.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			partRanges, err = BuildCNFIndexRange(sc, cols, lengths, cnfItems)
		} else {
			partRanges, err = BuildCNFIndexRange(sc, cols, lengths, []expression.Expression{arg})
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		totalRanges = append(totalRanges, partRanges...)
	}
	return unionNewRanges(totalRanges)
}

type sortObject struct {
	originalValue *NewRange
	encodedStart  []byte
	encodedEnd    []byte
}

func unionNewRanges(rangesInternal []*NewRange) ([]*NewRange, error) {
	objects := make([]*sortObject, 0, len(rangesInternal))
	for _, ran := range rangesInternal {
		left, err := codec.EncodeKey(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			left = kv.Key(left).PrefixNext()
		}
		right, err := codec.EncodeKey(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			right = kv.Key(right).PrefixNext()
		}
		objects = append(objects, &sortObject{originalValue: ran, encodedStart: left, encodedEnd: right})
	}
	sort.Slice(objects, func(i, j int) bool {
		return bytes.Compare(objects[i].encodedStart, objects[j].encodedStart) < 0
	})
	rangesInternal = rangesInternal[:0]
	curObject := objects[0]
	for i := 1; i < len(objects); i++ {
		if bytes.Compare(curObject.encodedEnd, objects[i].encodedStart) >= 0 {
			if bytes.Compare(curObject.encodedEnd, objects[i].encodedEnd) < 0 {
				curObject.encodedEnd = objects[i].encodedEnd
				curObject.originalValue.HighVal = objects[i].originalValue.HighVal
				curObject.originalValue.HighExclude = objects[i].originalValue.HighExclude
			}
		} else {
			rangesInternal = append(rangesInternal, curObject.originalValue)
			curObject = objects[i]
		}
	}
	rangesInternal = append(rangesInternal, curObject.originalValue)
	return rangesInternal, nil
}

func BuildIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, lengths []int,
	accessConditions []expression.Expression) ([]*NewRange, error) {
	if len(accessConditions) == 1 {
		if sf, ok := accessConditions[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			return BuildDNFIndexRange(sc, cols, lengths, accessConditions)
		}
	}
	return BuildCNFIndexRange(sc, cols, lengths, accessConditions)
}

func hasPrefix(lengths []int) bool {
	for _, l := range lengths {
		if l != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

func fixPrefixColRange(ranges []*NewRange, lengths []int) {
	for _, ran := range ranges {
		for i := 0; i < len(ran.LowVal); i++ {
			fixRangeDatum(&ran.LowVal[i], lengths[i])
		}
		ran.LowExclude = false
		for i := 0; i < len(ran.HighVal); i++ {
			fixRangeDatum(&ran.HighVal[i], lengths[i])
		}
		ran.HighExclude = false
	}
}

func fixRangeDatum(v *types.Datum, length int) {
	// If this column is prefix and the prefix length is smaller than the range, cut it.
	if length != types.UnspecifiedLength && length < len(v.GetBytes()) {
		v.SetBytes(v.GetBytes()[:length])
	}
}
