// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

const (
	// DefPartialResult4MaxMinCountSize is kept for compatibility.
	DefPartialResult4MaxMinCountSize = int64(unsafe.Sizeof(partialResult4MaxMinCountInt{}))

	DefPartialResult4MaxMinCountIntSize           = int64(unsafe.Sizeof(partialResult4MaxMinCountInt{}))
	DefPartialResult4MaxMinCountUintSize          = int64(unsafe.Sizeof(partialResult4MaxMinCountUint{}))
	DefPartialResult4MaxMinCountDecimalSize       = int64(unsafe.Sizeof(partialResult4MaxMinCountDecimal{}))
	DefPartialResult4MaxMinCountFloat32Size       = int64(unsafe.Sizeof(partialResult4MaxMinCountFloat32{}))
	DefPartialResult4MaxMinCountFloat64Size       = int64(unsafe.Sizeof(partialResult4MaxMinCountFloat64{}))
	DefPartialResult4MaxMinCountTimeSize          = int64(unsafe.Sizeof(partialResult4MaxMinCountTime{}))
	DefPartialResult4MaxMinCountDurationSize      = int64(unsafe.Sizeof(partialResult4MaxMinCountDuration{}))
	DefPartialResult4MaxMinCountStringSize        = int64(unsafe.Sizeof(partialResult4MaxMinCountString{}))
	DefPartialResult4MaxMinCountJSONSize          = int64(unsafe.Sizeof(partialResult4MaxMinCountJSON{}))
	DefPartialResult4MaxMinCountVectorFloat32Size = int64(unsafe.Sizeof(partialResult4MaxMinCountVectorFloat32{}))
	DefPartialResult4MaxMinCountEnumSize          = int64(unsafe.Sizeof(partialResult4MaxMinCountEnum{}))
	DefPartialResult4MaxMinCountSetSize           = int64(unsafe.Sizeof(partialResult4MaxMinCountSet{}))
)

type baseMaxMinCountAggFunc struct {
	baseMaxMinAggFunc
	hasDistinct bool
}

func (e *baseMaxMinCountAggFunc) appendFinalResult(isNull bool, count int64, chk *chunk.Chunk) {
	if isNull {
		chk.AppendInt64(e.ordinal, 0)
		return
	}
	chk.AppendInt64(e.ordinal, count)
}

func (e *baseMaxMinCountAggFunc) shouldReplace(cmp int) bool {
	return e.isMax && cmp > 0 || !e.isMax && cmp < 0
}

func (e *baseMaxMinCountAggFunc) shouldAccumulate(cmp int) bool {
	return cmp == 0 && !e.hasDistinct
}

func buildMaxMinCount(ctx expression.EvalContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	if aggFuncDesc.Mode == aggregation.DedupMode {
		return nil
	}

	argTp := aggFuncDesc.Args[0].GetType(ctx)
	base := baseMaxMinCountAggFunc{
		baseMaxMinAggFunc: baseMaxMinAggFunc{
			baseAggFunc: baseAggFunc{
				args:    aggFuncDesc.Args,
				ordinal: ordinal,
				retTp:   aggFuncDesc.RetTp,
			},
			isMax:    isMax,
			collator: collate.GetCollator(argTp.GetCollate()),
		},
		hasDistinct: aggFuncDesc.HasDistinct,
	}

	evalType, fieldType := argTp.EvalType(), argTp
	if fieldType.GetType() == mysql.TypeBit {
		evalType = types.ETString
	}

	switch fieldType.GetType() {
	case mysql.TypeEnum:
		return &maxMinCount4Enum{base}
	case mysql.TypeSet:
		return &maxMinCount4Set{base}
	}

	switch evalType {
	case types.ETInt:
		if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
			return &maxMinCount4Uint{base}
		}
		return &maxMinCount4Int{base}
	case types.ETReal:
		switch fieldType.GetType() {
		case mysql.TypeFloat:
			return &maxMinCount4Float32{base}
		case mysql.TypeDouble:
			return &maxMinCount4Float64{base}
		}
	case types.ETDecimal:
		return &maxMinCount4Decimal{base}
	case types.ETString:
		return &maxMinCount4String{baseMaxMinCountAggFunc: base}
	case types.ETDatetime, types.ETTimestamp:
		return &maxMinCount4Time{base}
	case types.ETDuration:
		return &maxMinCount4Duration{base}
	case types.ETJson:
		return &maxMinCount4JSON{base}
	case types.ETVectorFloat32:
		return &maxMinCount4VectorFloat32{base}
	}

	return nil
}

const (
	DefPartialResult4MaxMinCountSlidingSize = int64(unsafe.Sizeof(partialResult4MaxMinCountSliding{}))
	DefMaxMinCountDequeSize                 = int64(unsafe.Sizeof(minMaxCountDeque{}))
)

type maxMinCountDequeItem struct {
	item any
	idxs []uint64
}

type minMaxCountDeque struct {
	items   []maxMinCountDequeItem
	isMax   bool
	cmpFunc func(i, j any) int
}

func newMinMaxCountDeque(isMax bool, cmpFunc func(i, j any) int) *minMaxCountDeque {
	return &minMaxCountDeque{
		items:   make([]maxMinCountDequeItem, 0, 64),
		isMax:   isMax,
		cmpFunc: cmpFunc,
	}
}

func (d *minMaxCountDeque) reset() {
	d.items = d.items[:0]
}

func (d *minMaxCountDeque) isEmpty() bool {
	return len(d.items) == 0
}

func (d *minMaxCountDeque) enqueue(idx uint64, item any) {
	for !d.isEmpty() {
		backIdx := len(d.items) - 1
		cmp := d.cmpFunc(item, d.items[backIdx].item)
		if (d.isMax && cmp > 0) || (!d.isMax && cmp < 0) {
			d.items = d.items[:backIdx]
			continue
		}
		if cmp == 0 {
			d.items[backIdx].idxs = append(d.items[backIdx].idxs, idx)
			return
		}
		break
	}
	d.items = append(d.items, maxMinCountDequeItem{
		item: item,
		idxs: []uint64{idx},
	})
}

func (d *minMaxCountDeque) dequeue(boundary uint64) {
	for !d.isEmpty() {
		front := &d.items[0]
		firstAlive := 0
		for firstAlive < len(front.idxs) && front.idxs[firstAlive] <= boundary {
			firstAlive++
		}
		if firstAlive == 0 {
			return
		}
		front.idxs = front.idxs[firstAlive:]
		if len(front.idxs) > 0 {
			return
		}
		d.items = d.items[1:]
	}
}

func (d *minMaxCountDeque) frontCount() int {
	if d.isEmpty() {
		return 0
	}
	return len(d.items[0].idxs)
}

type partialResult4MaxMinCountSliding struct {
	deque  *minMaxCountDeque
	count  int64
	isNull bool
}

type maxMinCount4Sliding struct {
	baseMaxMinCountAggFunc
	eval  func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error)
	cmp   func(i, j any) int
	clone func(v any) any
	windowInfo
}

func (*maxMinCount4Sliding) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountSliding{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountSlidingSize + DefMaxMinCountDequeSize
}

func (e *maxMinCount4Sliding) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountSliding)(pr)
	p.count = 0
	p.isNull = true
	if p.deque != nil {
		p.deque.reset()
		return
	}
	p.deque = newMinMaxCountDeque(e.isMax, e.cmp)
}

func (e *maxMinCount4Sliding) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountSliding)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Sliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountSliding)(pr)
	if p.deque == nil {
		p.deque = newMinMaxCountDeque(e.isMax, e.cmp)
	}
	for i, row := range rowsInGroup {
		item, isNull, err := e.eval(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.deque.enqueue(uint64(i)+e.start, e.clone(item))
	}
	e.refreshCount(p)
	return 0, nil
}

func (e *maxMinCount4Sliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinCountSliding)(pr)
	if p.deque == nil {
		p.deque = newMinMaxCountDeque(e.isMax, e.cmp)
	}
	for i := uint64(0); i < shiftEnd; i++ {
		item, isNull, err := e.eval(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.deque.enqueue(lastEnd+i, e.clone(item))
	}
	if lastStart+shiftStart >= 1 {
		p.deque.dequeue(lastStart + shiftStart - 1)
	}
	e.refreshCount(p)
	return nil
}

func (e *maxMinCount4Sliding) refreshCount(p *partialResult4MaxMinCountSliding) {
	if p.deque == nil || p.deque.isEmpty() {
		p.isNull = true
		p.count = 0
		return
	}
	p.isNull = false
	p.count = int64(p.deque.frontCount())
}

func buildMaxMinCountInWindowFunction(ctx expression.EvalContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	base := buildMaxMinCount(ctx, aggFuncDesc, ordinal, isMax)
	if base == nil {
		return nil
	}

	build := func(baseFunc baseMaxMinCountAggFunc, eval func(AggFuncUpdateContext, chunk.Row) (any, bool, error), cmp func(i, j any) int, clone func(any) any) AggFunc {
		return &maxMinCount4Sliding{
			baseMaxMinCountAggFunc: baseFunc,
			eval:                   eval,
			cmp:                    cmp,
			clone:                  clone,
			windowInfo:             windowInfo{},
		}
	}

	switch b := base.(type) {
	case *maxMinCount4Int:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalInt(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				lhs, rhs := i.(int64), j.(int64)
				if lhs > rhs {
					return 1
				}
				if lhs < rhs {
					return -1
				}
				return 0
			},
			func(v any) any { return v },
		)
	case *maxMinCount4Uint:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalInt(sctx, row)
				return uint64(v), isNull, err
			},
			func(i, j any) int {
				lhs, rhs := i.(uint64), j.(uint64)
				if lhs > rhs {
					return 1
				}
				if lhs < rhs {
					return -1
				}
				return 0
			},
			func(v any) any { return v },
		)
	case *maxMinCount4Float32:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalReal(sctx, row)
				return float32(v), isNull, err
			},
			func(i, j any) int {
				lhs, rhs := i.(float32), j.(float32)
				if lhs > rhs {
					return 1
				}
				if lhs < rhs {
					return -1
				}
				return 0
			},
			func(v any) any { return v },
		)
	case *maxMinCount4Float64:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalReal(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				lhs, rhs := i.(float64), j.(float64)
				if lhs > rhs {
					return 1
				}
				if lhs < rhs {
					return -1
				}
				return 0
			},
			func(v any) any { return v },
		)
	case *maxMinCount4Decimal:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalDecimal(sctx, row)
				if isNull || err != nil {
					return types.MyDecimal{}, isNull, err
				}
				return *v, false, nil
			},
			func(i, j any) int {
				lhs, rhs := i.(types.MyDecimal), j.(types.MyDecimal)
				return lhs.Compare(&rhs)
			},
			func(v any) any { return v },
		)
	case *maxMinCount4String:
		collation := b.args[0].GetType(ctx).GetCollate()
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalString(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				return types.CompareString(i.(string), j.(string), collation)
			},
			func(v any) any { return stringutil.Copy(v.(string)) },
		)
	case *maxMinCount4Time:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalTime(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				return i.(types.Time).Compare(j.(types.Time))
			},
			func(v any) any { return v },
		)
	case *maxMinCount4Duration:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalDuration(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				return i.(types.Duration).Compare(j.(types.Duration))
			},
			func(v any) any { return v },
		)
	case *maxMinCount4JSON:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalJSON(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				return types.CompareBinaryJSON(i.(types.BinaryJSON), j.(types.BinaryJSON))
			},
			func(v any) any { return v.(types.BinaryJSON).Copy() },
		)
	case *maxMinCount4VectorFloat32:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				v, isNull, err := b.args[0].EvalVectorFloat32(sctx, row)
				return v, isNull, err
			},
			func(i, j any) int {
				return i.(types.VectorFloat32).Compare(j.(types.VectorFloat32))
			},
			func(v any) any { return v.(types.VectorFloat32).Clone() },
		)
	case *maxMinCount4Enum:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				d, err := b.args[0].Eval(sctx, row)
				if err != nil || d.IsNull() {
					return types.Enum{}, d.IsNull(), err
				}
				return d.GetMysqlEnum(), false, nil
			},
			func(i, j any) int {
				return b.collator.Compare(i.(types.Enum).Name, j.(types.Enum).Name)
			},
			func(v any) any { return v.(types.Enum).Copy() },
		)
	case *maxMinCount4Set:
		return build(b.baseMaxMinCountAggFunc,
			func(sctx AggFuncUpdateContext, row chunk.Row) (any, bool, error) {
				d, err := b.args[0].Eval(sctx, row)
				if err != nil || d.IsNull() {
					return types.Set{}, d.IsNull(), err
				}
				return d.GetMysqlSet(), false, nil
			},
			func(i, j any) int {
				return b.collator.Compare(i.(types.Set).Name, j.(types.Set).Name)
			},
			func(v any) any { return v.(types.Set).Copy() },
		)
	}
	return base
}

var _ SlidingWindowAggFunc = &maxMinCount4Sliding{}
var _ MaxMinSlidingWindowAggFunc = &maxMinCount4Sliding{}

type partialResult4MaxMinCountInt struct {
	val    int64
	count  int64
	isNull bool
}

type maxMinCount4Int struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Int) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountInt{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountIntSize
}

func (*maxMinCount4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountInt)(pr)
	p.val = 0
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Int) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountInt)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountInt)(pr)
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
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := 0
		if input > p.val {
			cmp = 1
		} else if input < p.val {
			cmp = -1
		}
		if e.shouldReplace(cmp) {
			p.val = input
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Int) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountInt)(src), (*partialResult4MaxMinCountInt)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := 0
	if p1.val > p2.val {
		cmp = 1
	} else if p1.val < p2.val {
		cmp = -1
	}
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Int) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountInt)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountInt(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Int) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Int) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountInt)(pr)
	success := helper.deserializePartialResult4MaxMinCountInt(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountUint struct {
	val    uint64
	count  int64
	isNull bool
}

type maxMinCount4Uint struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Uint) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountUint{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountUintSize
}

func (*maxMinCount4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountUint)(pr)
	p.val = 0
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Uint) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountUint)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Uint) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		u := uint64(input)
		if p.isNull {
			p.val = u
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := 0
		if u > p.val {
			cmp = 1
		} else if u < p.val {
			cmp = -1
		}
		if e.shouldReplace(cmp) {
			p.val = u
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Uint) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountUint)(src), (*partialResult4MaxMinCountUint)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := 0
	if p1.val > p2.val {
		cmp = 1
	} else if p1.val < p2.val {
		cmp = -1
	}
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Uint) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountUint)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountUint(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Uint) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Uint) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountUint)(pr)
	success := helper.deserializePartialResult4MaxMinCountUint(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountFloat32 struct {
	val    float32
	count  int64
	isNull bool
}

type maxMinCount4Float32 struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Float32) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountFloat32{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountFloat32Size
}

func (*maxMinCount4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountFloat32)(pr)
	p.val = 0
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Float32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountFloat32)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Float32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		v := float32(input)
		if p.isNull {
			p.val = v
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := 0
		if v > p.val {
			cmp = 1
		} else if v < p.val {
			cmp = -1
		}
		if e.shouldReplace(cmp) {
			p.val = v
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Float32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountFloat32)(src), (*partialResult4MaxMinCountFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := 0
	if p1.val > p2.val {
		cmp = 1
	} else if p1.val < p2.val {
		cmp = -1
	}
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Float32) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountFloat32)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountFloat32(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Float32) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Float32) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountFloat32)(pr)
	success := helper.deserializePartialResult4MaxMinCountFloat32(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountFloat64 struct {
	val    float64
	count  int64
	isNull bool
}

type maxMinCount4Float64 struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Float64) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountFloat64{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountFloat64Size
}

func (*maxMinCount4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountFloat64)(pr)
	p.val = 0
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountFloat64)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountFloat64)(pr)
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
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := 0
		if input > p.val {
			cmp = 1
		} else if input < p.val {
			cmp = -1
		}
		if e.shouldReplace(cmp) {
			p.val = input
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Float64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountFloat64)(src), (*partialResult4MaxMinCountFloat64)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := 0
	if p1.val > p2.val {
		cmp = 1
	} else if p1.val < p2.val {
		cmp = -1
	}
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Float64) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountFloat64)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountFloat64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Float64) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Float64) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountFloat64)(pr)
	success := helper.deserializePartialResult4MaxMinCountFloat64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountDecimal struct {
	val    types.MyDecimal
	count  int64
	isNull bool
}

type maxMinCount4Decimal struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Decimal) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountDecimal{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountDecimalSize
}

func (*maxMinCount4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountDecimal)(pr)
	p.val = types.MyDecimal{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountDecimal)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountDecimal)(pr)
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
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := input.Compare(&p.val)
		if e.shouldReplace(cmp) {
			p.val = *input
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Decimal) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountDecimal)(src), (*partialResult4MaxMinCountDecimal)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Decimal) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountDecimal)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountDecimal(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Decimal) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Decimal) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountDecimal)(pr)
	success := helper.deserializePartialResult4MaxMinCountDecimal(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountString struct {
	val    string
	count  int64
	isNull bool
}

type maxMinCount4String struct {
	baseMaxMinCountAggFunc
}

func (*maxMinCount4String) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountString{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountStringSize
}

func (*maxMinCount4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountString)(pr)
	p.val = ""
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4String) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountString)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountString)(pr)
	memDelta := int64(0)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = stringutil.Copy(input)
			p.count = 1
			p.isNull = false
			memDelta += int64(len(input))
			continue
		}
		tp := e.args[0].GetType(sctx)
		cmp := types.CompareString(input, p.val, tp.GetCollate())
		if e.shouldReplace(cmp) {
			oldLen := len(p.val)
			p.val = stringutil.Copy(input)
			p.count = 1
			memDelta += int64(len(input) - oldLen)
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return memDelta, nil
}

func (e *maxMinCount4String) MergePartialResult(ctx AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountString)(src), (*partialResult4MaxMinCountString)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	tp := e.args[0].GetType(ctx)
	cmp := types.CompareString(p1.val, p2.val, tp.GetCollate())
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4String) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountString)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountString(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4String) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4String) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountString)(pr)
	success := helper.deserializePartialResult4MaxMinCountString(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountTime struct {
	val    types.Time
	count  int64
	isNull bool
}

type maxMinCount4Time struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Time) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountTime{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountTimeSize
}

func (*maxMinCount4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountTime)(pr)
	p.val = types.Time{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Time) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountTime)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Time) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountTime)(pr)
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
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.shouldReplace(cmp) {
			p.val = input
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Time) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountTime)(src), (*partialResult4MaxMinCountTime)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Time) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Time) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Time) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountTime)(pr)
	success := helper.deserializePartialResult4MaxMinCountTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountDuration struct {
	val    types.Duration
	count  int64
	isNull bool
}

type maxMinCount4Duration struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Duration) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountDuration{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountDurationSize
}

func (*maxMinCount4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountDuration)(pr)
	p.val = types.Duration{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Duration) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountDuration)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountDuration)(pr)
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
			p.count = 1
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.shouldReplace(cmp) {
			p.val = input
			p.count = 1
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return 0, nil
}

func (e *maxMinCount4Duration) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountDuration)(src), (*partialResult4MaxMinCountDuration)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Duration) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountDuration)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountDuration(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Duration) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Duration) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountDuration)(pr)
	success := helper.deserializePartialResult4MaxMinCountDuration(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountJSON struct {
	val    types.BinaryJSON
	count  int64
	isNull bool
}

type maxMinCount4JSON struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4JSON) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountJSON{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountJSONSize
}

func (*maxMinCount4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountJSON)(pr)
	p.val = types.BinaryJSON{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4JSON) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountJSON)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4JSON) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountJSON)(pr)
	memDelta := int64(0)
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
			p.count = 1
			p.isNull = false
			memDelta += int64(len(input.Value))
			continue
		}
		cmp := types.CompareBinaryJSON(input, p.val)
		if e.shouldReplace(cmp) {
			oldLen := len(p.val.Value)
			p.val = input.Copy()
			p.count = 1
			memDelta += int64(len(input.Value) - oldLen)
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return memDelta, nil
}

func (e *maxMinCount4JSON) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountJSON)(src), (*partialResult4MaxMinCountJSON)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := types.CompareBinaryJSON(p1.val, p2.val)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4JSON) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountJSON)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountJSON(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4JSON) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4JSON) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountJSON)(pr)
	success := helper.deserializePartialResult4MaxMinCountJSON(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountVectorFloat32 struct {
	val    types.VectorFloat32
	count  int64
	isNull bool
}

type maxMinCount4VectorFloat32 struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4VectorFloat32) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountVectorFloat32{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountVectorFloat32Size
}

func (*maxMinCount4VectorFloat32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountVectorFloat32)(pr)
	p.val = types.VectorFloat32{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4VectorFloat32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountVectorFloat32)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4VectorFloat32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountVectorFloat32)(pr)
	memDelta := int64(0)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalVectorFloat32(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Clone()
			p.count = 1
			p.isNull = false
			memDelta += int64(input.EstimatedMemUsage())
			continue
		}
		cmp := input.Compare(p.val)
		if e.shouldReplace(cmp) {
			oldMem := p.val.EstimatedMemUsage()
			p.val = input.Clone()
			p.count = 1
			memDelta += int64(input.EstimatedMemUsage() - oldMem)
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return memDelta, nil
}

func (e *maxMinCount4VectorFloat32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountVectorFloat32)(src), (*partialResult4MaxMinCountVectorFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4VectorFloat32) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountVectorFloat32)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountVectorFloat32(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4VectorFloat32) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4VectorFloat32) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountVectorFloat32)(pr)
	success := helper.deserializePartialResult4MaxMinCountVectorFloat32(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountEnum struct {
	val    types.Enum
	count  int64
	isNull bool
}

type maxMinCount4Enum struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Enum) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountEnum{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountEnumSize
}

func (*maxMinCount4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountEnum)(pr)
	p.val = types.Enum{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Enum) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountEnum)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Enum) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountEnum)(pr)
	memDelta := int64(0)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		en := d.GetMysqlEnum()
		if p.isNull {
			p.val = en.Copy()
			p.count = 1
			p.isNull = false
			memDelta += int64(len(en.Name))
			continue
		}
		cmp := e.collator.Compare(en.Name, p.val.Name)
		if e.shouldReplace(cmp) {
			oldLen := len(p.val.Name)
			p.val = en.Copy()
			p.count = 1
			memDelta += int64(len(en.Name) - oldLen)
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return memDelta, nil
}

func (e *maxMinCount4Enum) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountEnum)(src), (*partialResult4MaxMinCountEnum)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := e.collator.Compare(p1.val.Name, p2.val.Name)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Enum) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountEnum)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountEnum(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Enum) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Enum) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountEnum)(pr)
	success := helper.deserializePartialResult4MaxMinCountEnum(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type partialResult4MaxMinCountSet struct {
	val    types.Set
	count  int64
	isNull bool
}

type maxMinCount4Set struct{ baseMaxMinCountAggFunc }

func (*maxMinCount4Set) AllocPartialResult() (PartialResult, int64) {
	p := &partialResult4MaxMinCountSet{isNull: true}
	return PartialResult(p), DefPartialResult4MaxMinCountSetSize
}

func (*maxMinCount4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinCountSet)(pr)
	p.val = types.Set{}
	p.count = 0
	p.isNull = true
}

func (e *maxMinCount4Set) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinCountSet)(pr)
	e.appendFinalResult(p.isNull, p.count, chk)
	return nil
}

func (e *maxMinCount4Set) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (int64, error) {
	p := (*partialResult4MaxMinCountSet)(pr)
	memDelta := int64(0)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		s := d.GetMysqlSet()
		if p.isNull {
			p.val = s.Copy()
			p.count = 1
			p.isNull = false
			memDelta += int64(len(s.Name))
			continue
		}
		cmp := e.collator.Compare(s.Name, p.val.Name)
		if e.shouldReplace(cmp) {
			oldLen := len(p.val.Name)
			p.val = s.Copy()
			p.count = 1
			memDelta += int64(len(s.Name) - oldLen)
		} else if e.shouldAccumulate(cmp) {
			p.count++
		}
	}
	return memDelta, nil
}

func (e *maxMinCount4Set) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (int64, error) {
	p1, p2 := (*partialResult4MaxMinCountSet)(src), (*partialResult4MaxMinCountSet)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := e.collator.Compare(p1.val.Name, p2.val.Name)
	if e.shouldReplace(cmp) {
		p2.val = p1.val
		p2.count = p1.count
		p2.isNull = false
	} else if e.shouldAccumulate(cmp) {
		p2.count += p1.count
	}
	return 0, nil
}

func (e *maxMinCount4Set) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinCountSet)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinCountSet(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMinCount4Set) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMinCount4Set) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinCountSet)(pr)
	success := helper.deserializePartialResult4MaxMinCountSet(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
