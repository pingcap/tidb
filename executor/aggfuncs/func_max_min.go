// Copyright 2018 PingCAP, Inc.
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

package aggfuncs

import (
	"container/heap"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

type maxMinHeap struct {
	data    []interface{}
	h       heap.Interface
	varSet  map[interface{}]int64
	isMax   bool
	cmpFunc func(i, j interface{}) int
}

func newMaxMinHeap(isMax bool, cmpFunc func(i, j interface{}) int) *maxMinHeap {
	h := &maxMinHeap{
		data:    make([]interface{}, 0),
		varSet:  make(map[interface{}]int64),
		isMax:   isMax,
		cmpFunc: cmpFunc,
	}
	return h
}

func (h *maxMinHeap) Len() int { return len(h.data) }
func (h *maxMinHeap) Less(i, j int) bool {
	if h.isMax {
		return h.cmpFunc(h.data[i], h.data[j]) > 0
	}
	return h.cmpFunc(h.data[i], h.data[j]) < 0
}
func (h *maxMinHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *maxMinHeap) Push(x interface{}) {
	h.data = append(h.data, x)
}
func (h *maxMinHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

func (h *maxMinHeap) Top() (val interface{}, isEmpty bool) {
retry:
	if h.Len() == 0 {
		return nil, true
	}
	top := h.data[0]
	if h.varSet[top] == 0 {
		_ = heap.Pop(h)
		goto retry
	}
	return top, false
}

func (h *maxMinHeap) Append(val interface{}) {
	h.varSet[val]++
	if h.varSet[val] == 1 {
		heap.Push(h, val)
	}
}
func (h *maxMinHeap) Remove(val interface{}) {
	if h.varSet[val] > 0 {
		h.varSet[val]--
	} else {
		panic("remove a not exist value")
	}
}
func (h *maxMinHeap) AppendMyDecimal(val types.MyDecimal) error {
	key, err := val.ToHashKey()
	if err != nil {
		return err
	}
	h.varSet[key]++
	if h.varSet[key] == 1 {
		heap.Push(h, val)
	}
	return nil
}
func (h *maxMinHeap) RemoveMyDecimal(val types.MyDecimal) error {
	key, err := val.ToHashKey()
	if err != nil {
		return err
	}
	if h.varSet[key] > 0 {
		h.varSet[key]--
	} else {
		panic("remove a not exist value")
	}
	return nil
}
func (h *maxMinHeap) Reset() {
	h.data = h.data[:0]
	h.varSet = make(map[interface{}]int64)
}

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
	// maxMinHeap is an ordered queue, using to evaluate the maximum or minimum value in a sliding window.
	heap *maxMinHeap
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
	heap   *maxMinHeap
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
	heap   *maxMinHeap
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
	heap   *maxMinHeap
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
	heap   *maxMinHeap
}

type partialResult4Time struct {
	val    types.Time
	isNull bool
	heap   *maxMinHeap
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
	heap   *maxMinHeap
}

type partialResult4MaxMinString struct {
	val    string
	isNull bool
}

type partialResult4MaxMinJSON struct {
	val    json.BinaryJSON
	isNull bool
}

type baseMaxMinAggFunc struct {
	baseAggFunc

	isMax bool
}

type maxMin4Int struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareInt64(i.(int64), j.(int64))
	})
	return PartialResult(p), 0
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinInt)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Int) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Uint) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareUint64(i.(uint64), j.(uint64))
	})
	return PartialResult(p), 0
}

func (e *maxMin4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Uint) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinUint)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Uint) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(uint64(input))
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Uint) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinUint)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(uint64(input))
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(uint64(input))
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Uint) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareFloat64(float64(i.(float32)), float64(j.(float32)))
	})
	return PartialResult(p), 0
}

func (e *maxMin4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(float32(input))
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Float32) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(float32(input))
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(float32(input))
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float32) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareFloat64(i.(float64), j.(float64))
	})
	return PartialResult(p), 0
}

func (e *maxMin4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Float64) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		src := i.(types.MyDecimal)
		dst := j.(types.MyDecimal)
		return src.Compare(&dst)
	})
	return PartialResult(p), 0
}

func (e *maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if err := p.heap.AppendMyDecimal(*input); err != nil {
			return 0, err
		}
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Decimal) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if err := p.heap.AppendMyDecimal(*input); err != nil {
			return err
		}
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if err := p.heap.RemoveMyDecimal(*input); err != nil {
			return err
		}
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (e *maxMin4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p), 0
}

func (e *maxMin4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
}

func (e *maxMin4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinString)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *maxMin4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	tp := e.args[0].GetType()
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			// The string returned by `EvalString` may be referenced to an underlying buffer,
			// for example ‘Chunk’, which could be reset and reused multiply times.
			// We have to deep copy that string to avoid some potential risks
			// when the content of that underlying buffer changed.
			p.val = stringutil.Copy(input)
			p.isNull = false
			continue
		}
		cmp := types.CompareString(input, p.val, tp.Collate)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = stringutil.Copy(input)
		}
	}
	return 0, nil
}

func (e *maxMin4String) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	tp := e.args[0].GetType()
	cmp := types.CompareString(p1.val, p2.val, tp.Collate)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4Time)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		src := i.(types.Time)
		dst := j.(types.Time)
		return src.Compare(dst)
	})
	return PartialResult(p), 0
}

func (e *maxMin4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Time)(pr)
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Time) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Time)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Time)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Time) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Time)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalTime(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalTime(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Time) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4Time)(src), (*partialResult4Time)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		src := i.(types.Duration)
		dst := j.(types.Duration)
		return src.Compare(dst)
	})
	return PartialResult(p), 0
}

func (e *maxMin4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
	p.heap.Reset()
}

func (e *maxMin4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDuration)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4Duration) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDuration)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalDuration(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalDuration(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (e *maxMin4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p), 0
}

func (e *maxMin4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinJSON)(pr)
	p.isNull = true
}

func (e *maxMin4JSON) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinJSON)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *maxMin4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Copy()
			p.isNull = false
			continue
		}
		cmp := json.CompareBinary(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = input.Copy()
		}
	}
	return 0, nil
}

func (e *maxMin4JSON) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := json.CompareBinary(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return 0, nil
}
