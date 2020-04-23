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
	"fmt"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"sort"
)

type maxMinQueue struct {
	queue   []interface{}
	counter map[interface{}]int64
	isMax   bool
	cmpFunc func(i, j interface{}) int
}

func newMaxMinQueue(isMax bool) *maxMinQueue {
	return &maxMinQueue{
		queue:   make([]interface{}, 0),
		counter: make(map[interface{}]int64),
		isMax:   isMax,
	}
}

func (m *maxMinQueue) Resort() {
	if len(m.queue) == 0 {
		return
	}
	sort.Slice(m.queue, func(i, j int) bool {
		if m.isMax {
			return m.cmpFunc(m.queue[i], m.queue[j]) > 0
		}
		return m.cmpFunc(m.queue[i], m.queue[j]) < 0
	})
}

func (m *maxMinQueue) Top() (val interface{}, isEmpty bool) {
	if len(m.queue) == 0 {
		return nil, true
	}
	fmt.Println("top", m.queue[0])
	return m.queue[0], false
}

func (m *maxMinQueue) Del(val interface{}) {
	for i, qVal := range m.queue {
		if m.cmpFunc(qVal, val) == 0 {
			if i < len(m.queue) {
				m.queue = append(m.queue[:i], m.queue[i+1:]...)
			} else {
				m.queue = m.queue[:i]
			}
		}
	}
	fmt.Println("Del", val, m.queue, m.counter)
}

func (m *maxMinQueue) Push(val interface{}) {
	if v, ok := m.counter[val]; ok {
		m.counter[val] = v + 1
	} else {
		m.counter[val] = 1
		m.queue = append(m.queue, val)
		m.Resort()
	}
	fmt.Println("Push", val, m.queue, m.counter)
}

func (m *maxMinQueue) Pop(val interface{}) {
	v, ok := m.counter[val]
	if ok {
		m.counter[val] = v - 1
		if v == 1 {
			m.Del(val)
			delete(m.counter, val)
			m.Resort()
		}
	}
	fmt.Println("Pop", val, m.queue, m.counter)
}

func (m *maxMinQueue) Reset() {
	m.queue = m.queue[:0]
	m.counter = make(map[interface{}]int64)
}

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
	queue  *maxMinQueue
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
	queue  *maxMinQueue
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
	queue  *maxMinQueue
}

type partialResult4Time struct {
	val    types.Time
	isNull bool
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
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

func (e *maxMin4Int) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	p.queue = newMaxMinQueue(e.isMax)
	p.queue.cmpFunc = func(i, j interface{}) int {
		return types.CompareInt64(i.(int64), j.(int64))
	}
	return PartialResult(p)
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
	p.queue.Reset()
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

func (e *maxMin4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(input)
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Int) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Pop(input)
	}
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(input)
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Uint) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
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

func (e *maxMin4Uint) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4Uint) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float32) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	p.queue = newMaxMinQueue(e.isMax)
	p.queue.cmpFunc = func(i, j interface{}) int {
		return types.CompareFloat64(float64(i.(float32)), float64(j.(float32)))
	}
	return PartialResult(p)
}

func (e *maxMin4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
	p.queue.Reset()
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

func (e *maxMin4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(float32(input))
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float32) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Pop(float32(input))
	}
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(float32(input))
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float32) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float64) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	p.queue = newMaxMinQueue(e.isMax)
	p.queue.cmpFunc = func(i, j interface{}) int {
		return types.CompareFloat64(i.(float64), j.(float64))
	}
	return PartialResult(p)
}

func (e *maxMin4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
	p.queue.Reset()
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

func (e *maxMin4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(input)
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float64) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Pop(input)
	}
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.queue.Push(input)
	}
	if val, isEmpty := p.queue.Top(); !isEmpty {
		p.val = val.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

func (e *maxMin4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Decimal) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
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

func (e *maxMin4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (e *maxMin4String) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p)
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

func (e *maxMin4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinString)(pr)
	tp := e.args[0].GetType()
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4String) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	tp := e.args[0].GetType()
	cmp := types.CompareString(p1.val, p2.val, tp.Collate)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Time) AllocPartialResult() PartialResult {
	p := new(partialResult4Time)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Time)(pr)
	p.isNull = true
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

func (e *maxMin4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Time)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4Time) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Time)(src), (*partialResult4Time)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Duration) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
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

func (e *maxMin4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (e *maxMin4JSON) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p)
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

func (e *maxMin4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return err
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
	return nil
}

func (e *maxMin4JSON) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := json.CompareBinary(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return nil
}
