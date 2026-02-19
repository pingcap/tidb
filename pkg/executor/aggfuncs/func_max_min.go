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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// NewDeque inits a new MinMaxDeque
func NewDeque(isMax bool, cmpFunc func(i, j any) int) *MinMaxDeque {
	return &MinMaxDeque{[]Pair{}, isMax, cmpFunc}
}

// MinMaxDeque is an array based double end queue
type MinMaxDeque struct {
	Items   []Pair
	IsMax   bool
	cmpFunc func(i, j any) int
}

// PushBack pushes Idx and Item(wrapped in Pair) to the end of MinMaxDeque
func (d *MinMaxDeque) PushBack(idx uint64, item any) {
	d.Items = append(d.Items, Pair{item, idx})
}

// PopFront pops an Item from the front of MinMaxDeque
func (d *MinMaxDeque) PopFront() error {
	if len(d.Items) <= 0 {
		return errors.New("Pop front when MinMaxDeque is empty")
	}
	d.Items = d.Items[1:]
	return nil
}

// PopBack pops an Item from the end of MinMaxDeque
func (d *MinMaxDeque) PopBack() error {
	i := len(d.Items) - 1
	if i < 0 {
		return errors.New("Pop back when MinMaxDeque is empty")
	}
	d.Items = d.Items[:i]
	return nil
}

// Back returns the element at the end of MinMaxDeque, and whether reached end of deque
func (d *MinMaxDeque) Back() (Pair, bool) {
	i := len(d.Items) - 1
	if i < 0 {
		return Pair{}, true
	}
	return d.Items[i], false
}

// Front returns the element at the front of MinMaxDeque, and whether reached end of deque
func (d *MinMaxDeque) Front() (Pair, bool) {
	if len(d.Items) <= 0 {
		return Pair{}, true
	}
	return d.Items[0], false
}

// IsEmpty returns if MinMaxDeque is empty
func (d *MinMaxDeque) IsEmpty() bool {
	return len(d.Items) == 0
}

// Pair pairs items and their indices in MinMaxDeque
type Pair struct {
	Item any
	Idx  uint64
}

// Reset resets the deque for a MaxMinSlidingWindowAggFunc
func (d *MinMaxDeque) Reset() {
	d.Items = d.Items[:0]
}

// Dequeue pops out element from the front, if element's index is out of boundary, i.e. the leftmost element index
func (d *MinMaxDeque) Dequeue(boundary uint64) error {
	for !d.IsEmpty() {
		frontEle, isEnd := d.Front()
		if isEnd {
			return errors.New("Dequeue empty deque")
		}
		if frontEle.Idx <= boundary {
			err := d.PopFront()
			if err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return nil
}

// Enqueue put Item at the back of queue, while popping any element that is lesser element in queue
func (d *MinMaxDeque) Enqueue(idx uint64, item any) error {
	for !d.IsEmpty() {
		pair, isEnd := d.Back()
		if isEnd {
			return errors.New("Dequeue empty deque")
		}

		cmp := d.cmpFunc(item, pair.Item)
		// 1. if MinMaxDeque aims for finding max and Item is equal or bigger than element at back
		// 2. if MinMaxDeque aims for finding min and Item is equal or smaller than element at back
		if !(cmp >= 0 && d.IsMax || cmp <= 0 && !d.IsMax) {
			break
		}
		err := d.PopBack()
		if err != nil {
			return err
		}
	}
	d.PushBack(idx, item)
	return nil
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
	// DefPartialResult4MaxMinTimeSize is the size of partialResult4MaxMinTime
	DefPartialResult4MaxMinTimeSize = int64(unsafe.Sizeof(partialResult4MaxMinTime{}))
	// DefPartialResult4MaxMinDurationSize is the size of partialResult4MaxMinDuration
	DefPartialResult4MaxMinDurationSize = int64(unsafe.Sizeof(partialResult4MaxMinDuration{}))
	// DefPartialResult4MaxMinStringSize is the size of partialResult4MaxMinString
	DefPartialResult4MaxMinStringSize = int64(unsafe.Sizeof(partialResult4MaxMinString{}))
	// DefPartialResult4MaxMinJSONSize is the size of partialResult4MaxMinJSON
	DefPartialResult4MaxMinJSONSize = int64(unsafe.Sizeof(partialResult4MaxMinJSON{}))
	// DefPartialResult4MaxMinVectorFloat32Size is the size of partialResult4MaxMinVectorFloat32
	DefPartialResult4MaxMinVectorFloat32Size = int64(unsafe.Sizeof(partialResult4MaxMinVectorFloat32{}))
	// DefPartialResult4MaxMinEnumSize is the size of partialResult4MaxMinEnum
	DefPartialResult4MaxMinEnumSize = int64(unsafe.Sizeof(partialResult4MaxMinEnum{}))
	// DefPartialResult4MaxMinSetSize is the size of partialResult4MaxMinSet
	DefPartialResult4MaxMinSetSize = int64(unsafe.Sizeof(partialResult4MaxMinSet{}))
	// DefMaxMinDequeSize is the size of maxMinHeap
	DefMaxMinDequeSize = int64(unsafe.Sizeof(MinMaxDeque{}))
)

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
	// deque is used for recording max/mins inside current window with sliding algorithm
	deque *MinMaxDeque
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinTime struct {
	val    types.Time
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinString struct {
	val    string
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinJSON struct {
	val    types.BinaryJSON
	isNull bool
}

type partialResult4MaxMinVectorFloat32 struct {
	val    types.VectorFloat32
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
	isMax    bool
	collator collate.Collator
}

// windowInfo is a struct used to store window related info
type windowInfo struct {
	start uint64
}

// SetWindowStart sets absolute start position of window
func (w *windowInfo) SetWindowStart(start uint64) {
	w.start = start
}
