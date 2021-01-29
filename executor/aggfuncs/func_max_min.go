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
	"github.com/pingcap/errors"
	"unsafe"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

// newDeque inits a new Deque
func newDeque(isMax bool) *Deque {
	return &Deque{[]Pair{}, isMax}
}

// Deque is an array based double end queue
type Deque struct {
	Items []Pair
	IsMax bool
}

// PushFront pushes idx and item(wrapped in Pair) to the front of Deque
func (d *Deque) PushFront(idx uint64, item interface{}) {
	p := Pair{item, idx}
	d.Items = append([]Pair{p}, d.Items...)
}

// PushBack pushes idx and item(wrapped in Pair) to the end of Deque
func (d *Deque) PushBack(idx uint64, item interface{}) {
	p := Pair{item, idx}
	d.Items = append(d.Items, p)
}

// PopFront pops an item from the front of Deque
func (d *Deque) PopFront() (interface{}, error) {
	if len(d.Items) <= 0 {
		return nil, errors.New("Pop front when deque is empty")
	}
	defer func() {
		d.Items = d.Items[1:]
	}()
	return d.Items[0], nil
}

// PopBack pops an item from the end of Deque
func (d *Deque) PopBack() (interface{}, error) {
	i := len(d.Items) - 1
	if i < 0 {
		return nil, errors.New("Pop back when deque is empty")
	}
	defer func() {
		d.Items = d.Items[:i]
	}()
	return d.Items[i], nil
}

// Back returns the element at the end of Deque
func (d *Deque) Back() (Pair, error) {
	i := len(d.Items) - 1
	if i < 0 {
		return Pair{}, errors.New("Cannot get back when deque is empty")
	}
	return d.Items[i], nil
}

// Front returns the element at the front of Deque
func (d *Deque) Front() (Pair, error) {
	if len(d.Items) <= 0 {
		return Pair{}, errors.New("Cannot get back when deque is empty")
	}
	return d.Items[0], nil
}

// IsEmpty returns if Deque is empty
func (d *Deque) IsEmpty() bool {
	if len(d.Items) == 0 {
		return true
	}
	return false
}

// Pair pairs items and their indices in deque
type Pair struct {
	item interface{}
	idx  uint64
}

// Dequeue pops out element from the front, if element's index is out of boundary, i.e. the leftmost element index
func (d *Deque) Dequeue(boundary uint64) error {
	for !d.IsEmpty() {
		frontEle, err := d.Front()
		if err != nil {
			return err
		}
		if frontEle.idx < boundary {
			_, err := d.PopFront()
			if err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return nil
}

// Enqueue put item at the back of queue, while popping any element that is lesser element in queue
func (d *Deque) Enqueue(idx uint64, item interface{}) error {
	if d.IsEmpty() {
		d.PushBack(idx, item)
		return nil
	}

	for !d.IsEmpty() {
		pair, err := d.Back()
		if err != nil {
			return err
		}

		var bigger bool
		switch it := pair.item; it.(type) {
		case int64:
			if item.(int64) > it.(int64) {
				bigger = true
			}
		case float64:
			if item.(int64) > it.(int64) {
				bigger = true
			}
		case types.MyDecimal:
			it, itemVal := it.(types.MyDecimal), item.(types.MyDecimal)
			if itemVal.Compare(&it) >= 0 {
				bigger = true
			}
		default:
			return errors.New("unsupported type")
		}

		// 1. if deque aims for finding max and item is bigger than element at back
		// 2. if deque aims for finding min and item is smaller than element at back
		if bigger && d.IsMax || !bigger && !d.IsMax {
			_, err := d.PopBack()
			if err != nil {
				return nil
			}
		} else {
			break
		}
	}
	d.PushBack(idx, item)
	return nil
}

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

func (h *maxMinHeap) Reset() {
	h.data = h.data[:0]
	h.varSet = make(map[interface{}]int64)
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

func (h *maxMinHeap) AppendMyDecimal(val types.MyDecimal) error {
	key, err := val.ToHashKey()
	if err != nil {
		return err
	}
	h.varSet[string(key)]++
	if h.varSet[string(key)] == 1 {
		heap.Push(h, val)
	}
	return nil
}
func (h *maxMinHeap) RemoveMyDecimal(val types.MyDecimal) error {
	key, err := val.ToHashKey()
	if err != nil {
		return err
	}
	if h.varSet[string(key)] > 0 {
		h.varSet[string(key)]--
	} else {
		panic("remove a not exist value")
	}
	return nil
}
func (h *maxMinHeap) TopDecimal() (val types.MyDecimal, isEmpty bool) {
retry:
	if h.Len() == 0 {
		return types.MyDecimal{}, true
	}
	top := h.data[0].(types.MyDecimal)
	key, err := top.ToHashKey()
	if err != nil {
		panic(err)
	}
	if h.varSet[string(key)] == 0 {
		_ = heap.Pop(h)
		goto retry
	}
	return top, false
}

const (
	// DefPartialResult4MaxMinIntSize is the size of partialResult4MaxMinInt
	DefPartialResult4MaxMinIntSize = int64(unsafe.Sizeof(partialResult4MaxMinInt{}))
	// DefPartialResult4MaxMinUintSize is the size of partialResult4MaxMinUint
	DefPartialResult4MaxMinUintSize = int64(unsafe.Sizeof(partialResult4MaxMinUint{}))
	// DefPartialResult4MaxMinDecimalSize is the size of partialResult4MaxMinDecimal
	DefPartialResult4MaxMinDecimalSize = int64(unsafe.Sizeof(partialResult4MaxMinDecimal{}))
	// DefPartialResult4MaxMinFloat32Size is the size of partialResult4MaxMinFloat32
	DefPartialResult4MaxMinFloat32Size = int64(unsafe.Sizeof(partialResult4MaxMinFloat32{}))
	// DefPartialResult4MaxMinFloat64Size is the size of partialResult4MaxMinFloat64
	DefPartialResult4MaxMinFloat64Size = int64(unsafe.Sizeof(partialResult4MaxMinFloat64{}))
	// DefPartialResult4TimeSize is the size of partialResult4Time
	DefPartialResult4TimeSize = int64(unsafe.Sizeof(partialResult4Time{}))
	// DefPartialResult4MaxMinDurationSize is the size of partialResult4MaxMinDuration
	DefPartialResult4MaxMinDurationSize = int64(unsafe.Sizeof(partialResult4MaxMinDuration{}))
	// DefPartialResult4MaxMinStringSize is the size of partialResult4MaxMinString
	DefPartialResult4MaxMinStringSize = int64(unsafe.Sizeof(partialResult4MaxMinString{}))
	// DefPartialResult4MaxMinJSONSize is the size of partialResult4MaxMinJSON
	DefPartialResult4MaxMinJSONSize = int64(unsafe.Sizeof(partialResult4MaxMinJSON{}))
	// DefPartialResult4MaxMinEnumSize is the size of partialResult4MaxMinEnum
	DefPartialResult4MaxMinEnumSize = int64(unsafe.Sizeof(partialResult4MaxMinEnum{}))
	// DefPartialResult4MaxMinSetSize is the size of partialResult4MaxMinSet
	DefPartialResult4MaxMinSetSize = int64(unsafe.Sizeof(partialResult4MaxMinSet{}))
)

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
	deque  *Deque
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
	heap   *maxMinHeap
}

type partialResult4MaxMinJSON struct {
	val    json.BinaryJSON
	isNull bool
}

type partialResult4MaxMinEnum struct {
	val    types.Enum
	isNull bool
}

type partialResult4MaxMinSet struct {
	val    types.Set
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
	p.deque = newDeque(e.isMax)
	return PartialResult(p), DefPartialResult4MaxMinIntSize
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
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
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
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

type maxMin4IntSliding struct {
	maxMin4Int
}

func (e *maxMin4IntSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.deque.Enqueue(uint64(i), input)
	}
	if val, err := p.deque.Front(); err == nil {
		p.val = val.item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4IntSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEmpty := p.deque.Front(); isEmpty == nil {
		p.val = val.item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
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
	return PartialResult(p), DefPartialResult4MaxMinUintSize
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
		uintVal := uint64(input)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		if e.isMax && uintVal > p.val || !e.isMax && uintVal < p.val {
			p.val = uintVal
		}
	}
	return 0, nil
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

type maxMin4UintSliding struct {
	maxMin4Uint
}

func (e *maxMin4UintSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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

func (e *maxMin4UintSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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
	return PartialResult(p), DefPartialResult4MaxMinFloat32Size
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
		f := float32(input)
		if p.isNull {
			p.val = f
			p.isNull = false
			continue
		}
		if e.isMax && f > p.val || !e.isMax && f < p.val {
			p.val = f
		}
	}
	return 0, nil
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

type maxMin4Float32Sliding struct {
	maxMin4Float32
}

func (e *maxMin4Float32Sliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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

func (e *maxMin4Float32Sliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareFloat64(i.(float64), j.(float64))
	})
	return PartialResult(p), DefPartialResult4MaxMinFloat64Size
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
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
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

type maxMin4Float64Sliding struct {
	maxMin4Float64
}

func (e *maxMin4Float64Sliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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

func (e *maxMin4Float64Sliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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
	return PartialResult(p), DefPartialResult4MaxMinDecimalSize
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
	err := p.val.Round(&p.val, e.frac, types.ModeHalfEven)
	if err != nil {
		return err
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
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		cmp := input.Compare(&p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = *input
		}
	}
	return 0, nil
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

type maxMin4DecimalSliding struct {
	maxMin4Decimal
}

func (e *maxMin4DecimalSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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
	if val, isEmpty := p.heap.TopDecimal(); !isEmpty {
		p.val = val
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4DecimalSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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
	if val, isEmpty := p.heap.TopDecimal(); !isEmpty {
		p.val = val
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (e *maxMin4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	tp := e.args[0].GetType()
	p.heap = newMaxMinHeap(e.isMax, func(i, j interface{}) int {
		return types.CompareString(i.(string), j.(string), tp.Collate)
	})
	return PartialResult(p), DefPartialResult4MaxMinStringSize
}

func (e *maxMin4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
	p.heap.Reset()
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
			return memDelta, err
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
			memDelta += int64(len(input))
			p.isNull = false
			continue
		}
		cmp := types.CompareString(input, p.val, tp.Collate)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			oldMem := len(p.val)
			newMem := len(input)
			memDelta += int64(newMem - oldMem)
			p.val = stringutil.Copy(input)
		}
	}
	return memDelta, nil
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

type maxMin4StringSliding struct {
	maxMin4String
}

func (e *maxMin4StringSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4StringSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinString)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalString(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Append(input)
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalString(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.heap.Remove(input)
	}
	if val, isEmpty := p.heap.Top(); !isEmpty {
		p.val = val.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
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
	return PartialResult(p), DefPartialResult4TimeSize
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
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
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

type maxMin4TimeSliding struct {
	maxMin4Time
}

func (e *maxMin4TimeSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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

func (e *maxMin4TimeSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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
	return PartialResult(p), DefPartialResult4MaxMinDurationSize
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
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
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

type maxMin4DurationSliding struct {
	maxMin4Duration
}

func (e *maxMin4DurationSliding) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
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

func (e *maxMin4DurationSliding) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
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

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (e *maxMin4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinJSONSize
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
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Copy()
			memDelta += int64(len(input.Value))
			p.isNull = false
			continue
		}
		cmp := json.CompareBinary(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			oldMem := len(p.val.Value)
			newMem := len(input.Value)
			memDelta += int64(newMem - oldMem)
			p.val = input.Copy()
		}
	}
	return memDelta, nil
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

type maxMin4Enum struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinEnum)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinEnumSize
}

func (e *maxMin4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinEnum)(pr)
	p.isNull = true
}

func (e *maxMin4Enum) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinEnum)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Enum) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinEnum)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlEnum().Copy()
			memDelta += int64(len(d.GetMysqlEnum().Name))
			p.isNull = false
			continue
		}
		en := d.GetMysqlEnum()
		if e.isMax && en.Name > p.val.Name || !e.isMax && en.Name < p.val.Name {
			oldMem := len(p.val.Name)
			newMem := len(en.Name)
			memDelta += int64(newMem - oldMem)
			p.val = en.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Enum) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinEnum)(src), (*partialResult4MaxMinEnum)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val.Name > p2.val.Name || !e.isMax && p1.val.Name < p2.val.Name {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Set struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinSet)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinSetSize
}

func (e *maxMin4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinSet)(pr)
	p.isNull = true
}

func (e *maxMin4Set) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinSet)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Set) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinSet)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlSet().Copy()
			memDelta += int64(len(d.GetMysqlSet().Name))
			p.isNull = false
			continue
		}
		s := d.GetMysqlSet()
		if e.isMax && s.Name > p.val.Name || !e.isMax && s.Name < p.val.Name {
			oldMem := len(p.val.Name)
			newMem := len(s.Name)
			memDelta += int64(newMem - oldMem)
			p.val = s.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Set) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinSet)(src), (*partialResult4MaxMinSet)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val.Name > p2.val.Name || !e.isMax && p1.val.Name < p2.val.Name {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}
