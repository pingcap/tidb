// Copyright 2023 PingCAP, Inc.
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

package vecgroupchecker

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// VecGroupChecker is used to split a given chunk according to the `group by` expression in a vectorized manner
// It is usually used for streamAgg
type VecGroupChecker struct {
	ctx           expression.EvalContext
	releaseBuffer func(buf *chunk.Column)

	// set these functions for testing
	allocateBuffer func(evalType types.EvalType, capacity int) (*chunk.Column, error)
	lastRowDatums  []types.Datum

	// lastGroupKeyOfPrevChk is the groupKey of the last group of the previous chunk
	lastGroupKeyOfPrevChk []byte
	// firstGroupKey and lastGroupKey are used to store the groupKey of the first and last group of the current chunk
	firstGroupKey []byte
	lastGroupKey  []byte

	// firstRowDatums and lastRowDatums store the results of the expression evaluation
	// for the first and last rows of the current chunk in datum
	// They are used to encode to get firstGroupKey and lastGroupKey
	firstRowDatums []types.Datum

	// sameGroup is used to check whether the current row belongs to the same group as the previous row
	sameGroup []bool

	// groupOffset holds the offset of the last row in each group of the current chunk
	groupOffset  []int
	GroupByItems []expression.Expression

	// nextGroupID records the group id of the next group to be consumed
	nextGroupID int

	// groupCount is the count of groups in the current chunk
	groupCount int

	// vecEnabled indicates whether to use vectorized evaluation or not.
	vecEnabled bool
}

// NewVecGroupChecker creates a new VecGroupChecker
func NewVecGroupChecker(ctx expression.EvalContext, vecEnabled bool, items []expression.Expression) *VecGroupChecker {
	return &VecGroupChecker{
		ctx:          ctx,
		vecEnabled:   vecEnabled,
		GroupByItems: items,
		groupCount:   0,
		nextGroupID:  0,
		sameGroup:    make([]bool, 1024),
	}
}

// SplitIntoGroups splits a chunk into multiple groups which the row in the same group have the same groupKey
// `isFirstGroupSameAsPrev` indicates whether the groupKey of the first group of the newly passed chunk is equal to the groupKey of the last group left before
// TODO: Since all the group by items are only a column reference, guaranteed by building projection below aggregation, we can directly compare data in a chunk.
func (e *VecGroupChecker) SplitIntoGroups(chk *chunk.Chunk) (isFirstGroupSameAsPrev bool, err error) {
	// The numRows can not be zero. `fetchChild` is called before `splitIntoGroups` is called.
	// if numRows == 0, it will be returned in `fetchChild`. See `fetchChild` for more details.
	numRows := chk.NumRows()

	e.Reset()
	e.nextGroupID = 0
	if len(e.GroupByItems) == 0 {
		e.groupOffset = append(e.groupOffset, numRows)
		e.groupCount = 1
		return true, nil
	}

	for _, item := range e.GroupByItems {
		err = e.getFirstAndLastRowDatum(item, chk, numRows)
		if err != nil {
			return false, err
		}
	}
	ec := e.ctx.ErrCtx()
	e.firstGroupKey, err = codec.EncodeKey(e.ctx.Location(), e.firstGroupKey, e.firstRowDatums...)
	err = ec.HandleError(err)
	if err != nil {
		return false, err
	}

	e.lastGroupKey, err = codec.EncodeKey(e.ctx.Location(), e.lastGroupKey, e.lastRowDatums...)
	err = ec.HandleError(err)
	if err != nil {
		return false, err
	}

	if len(e.lastGroupKeyOfPrevChk) == 0 {
		isFirstGroupSameAsPrev = false
	} else {
		if bytes.Equal(e.lastGroupKeyOfPrevChk, e.firstGroupKey) {
			isFirstGroupSameAsPrev = true
		} else {
			isFirstGroupSameAsPrev = false
		}
	}

	if length := len(e.lastGroupKey); len(e.lastGroupKeyOfPrevChk) >= length {
		e.lastGroupKeyOfPrevChk = e.lastGroupKeyOfPrevChk[:length]
	} else {
		e.lastGroupKeyOfPrevChk = make([]byte, length)
	}
	copy(e.lastGroupKeyOfPrevChk, e.lastGroupKey)

	if bytes.Equal(e.firstGroupKey, e.lastGroupKey) {
		e.groupOffset = append(e.groupOffset, numRows)
		e.groupCount = 1
		return isFirstGroupSameAsPrev, nil
	}

	if cap(e.sameGroup) < numRows {
		e.sameGroup = make([]bool, 0, numRows)
	}
	e.sameGroup = append(e.sameGroup, false)
	for i := 1; i < numRows; i++ {
		e.sameGroup = append(e.sameGroup, true)
	}

	for _, item := range e.GroupByItems {
		err = e.evalGroupItemsAndResolveGroups(item, e.vecEnabled, chk, numRows)
		if err != nil {
			return false, err
		}
	}

	for i := 1; i < numRows; i++ {
		if !e.sameGroup[i] {
			e.groupOffset = append(e.groupOffset, i)
		}
	}
	e.groupOffset = append(e.groupOffset, numRows)
	e.groupCount = len(e.groupOffset)
	return isFirstGroupSameAsPrev, nil
}

func (e *VecGroupChecker) getFirstAndLastRowDatum(
	item expression.Expression, chk *chunk.Chunk, numRows int) (err error) {
	var firstRowDatum, lastRowDatum types.Datum
	tp := item.GetType(e.ctx)
	eType := tp.EvalType()
	switch eType {
	case types.ETInt:
		firstRowVal, firstRowIsNull, err := item.EvalInt(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalInt(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetInt64(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetInt64(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETReal:
		firstRowVal, firstRowIsNull, err := item.EvalReal(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalReal(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetFloat64(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetFloat64(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDecimal:
		firstRowVal, firstRowIsNull, err := item.EvalDecimal(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalDecimal(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstDatum := types.MyDecimal{}
			err := firstDatum.FromString(firstRowVal.ToString())
			if err != nil {
				return err
			}
			firstRowDatum.SetMysqlDecimal(&firstDatum)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastDatum := types.MyDecimal{}
			err := lastDatum.FromString(lastRowVal.ToString())
			if err != nil {
				return err
			}
			lastRowDatum.SetMysqlDecimal(&lastDatum)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDatetime, types.ETTimestamp:
		firstRowVal, firstRowIsNull, err := item.EvalTime(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalTime(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetMysqlTime(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetMysqlTime(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDuration:
		firstRowVal, firstRowIsNull, err := item.EvalDuration(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalDuration(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetMysqlDuration(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetMysqlDuration(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETJson:
		firstRowVal, firstRowIsNull, err := item.EvalJSON(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalJSON(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstRowDatum.SetMysqlJSON(firstRowVal.Copy())
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastRowDatum.SetMysqlJSON(lastRowVal.Copy())
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETString:
		firstRowVal, firstRowIsNull, err := item.EvalString(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalString(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstDatum := string([]byte(firstRowVal))
			firstRowDatum.SetString(firstDatum, tp.GetCollate())
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastDatum := string([]byte(lastRowVal))
			lastRowDatum.SetString(lastDatum, tp.GetCollate())
		} else {
			lastRowDatum.SetNull()
		}
	default:
		err = fmt.Errorf("invalid eval type %v", eType)
		return err
	}

	e.firstRowDatums = append(e.firstRowDatums, firstRowDatum)
	e.lastRowDatums = append(e.lastRowDatums, lastRowDatum)
	return err
}

// evalGroupItemsAndResolveGroups evaluates the chunk according to the expression item.
// And resolve the rows into groups according to the evaluation results
func (e *VecGroupChecker) evalGroupItemsAndResolveGroups(
	item expression.Expression, vecEnabled bool, chk *chunk.Chunk, numRows int) (err error) {
	tp := item.GetType(e.ctx)
	eType := tp.EvalType()
	if e.allocateBuffer == nil {
		e.allocateBuffer = expression.GetColumn
	}
	if e.releaseBuffer == nil {
		e.releaseBuffer = expression.PutColumn
	}
	col, err := e.allocateBuffer(eType, numRows)
	if err != nil {
		return err
	}
	defer e.releaseBuffer(col)
	err = expression.EvalExpr(e.ctx, vecEnabled, item, eType, chk, col)
	if err != nil {
		return err
	}

	previousIsNull := col.IsNull(0)
	switch eType {
	case types.ETInt:
		vals := col.Int64s()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETReal:
		vals := col.Float64s()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDecimal:
		vals := col.Decimals()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(&vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDatetime, types.ETTimestamp:
		vals := col.Times()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDuration:
		vals := col.GoDurations()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETJson:
		var previousKey, key types.BinaryJSON
		if !previousIsNull {
			previousKey = col.GetJSON(0)
		}
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if !isNull {
				key = col.GetJSON(i)
			}
			if e.sameGroup[i] {
				if isNull == previousIsNull {
					if !isNull && types.CompareBinaryJSON(previousKey, key) != 0 {
						e.sameGroup[i] = false
					}
				} else {
					e.sameGroup[i] = false
				}
			}
			if !isNull {
				previousKey = key
			}
			previousIsNull = isNull
		}
	case types.ETString:
		previousKey := codec.ConvertByCollationStr(col.GetString(0), tp)
		for i := 1; i < numRows; i++ {
			key := codec.ConvertByCollationStr(col.GetString(i), tp)
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				if isNull != previousIsNull || previousKey != key {
					e.sameGroup[i] = false
				}
			}
			previousKey = key
			previousIsNull = isNull
		}
	default:
		err = fmt.Errorf("invalid eval type %v", eType)
	}
	if err != nil {
		return err
	}

	return err
}

// GetNextGroup returns the begin and end position of the next group.
func (e *VecGroupChecker) GetNextGroup() (begin, end int) {
	if e.nextGroupID == 0 {
		begin = 0
	} else {
		begin = e.groupOffset[e.nextGroupID-1]
	}
	end = e.groupOffset[e.nextGroupID]
	e.nextGroupID++
	return begin, end
}

// IsExhausted returns true if there is no more group to check.
func (e *VecGroupChecker) IsExhausted() bool {
	return e.nextGroupID >= e.groupCount
}

// Reset resets the group checker.
func (e *VecGroupChecker) Reset() {
	if e.groupOffset != nil {
		e.groupOffset = e.groupOffset[:0]
	}
	if e.sameGroup != nil {
		e.sameGroup = e.sameGroup[:0]
	}
	if e.firstGroupKey != nil {
		e.firstGroupKey = e.firstGroupKey[:0]
	}
	if e.lastGroupKey != nil {
		e.lastGroupKey = e.lastGroupKey[:0]
	}
	if e.firstRowDatums != nil {
		e.firstRowDatums = e.firstRowDatums[:0]
	}
	if e.lastRowDatums != nil {
		e.lastRowDatums = e.lastRowDatums[:0]
	}
}

// GroupCount returns the number of groups.
func (e *VecGroupChecker) GroupCount() int {
	return e.groupCount
}
