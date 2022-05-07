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
	"bytes"
	"container/heap"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/set"
)

const (
	// DefPartialResult4GroupConcatSize is the size of partialResult4GroupConcat
	DefPartialResult4GroupConcatSize = int64(unsafe.Sizeof(partialResult4GroupConcat{}))
	// DefPartialResult4GroupConcatDistinctSize is the size of partialResult4GroupConcatDistinct
	DefPartialResult4GroupConcatDistinctSize = int64(unsafe.Sizeof(partialResult4GroupConcatDistinct{}))
	// DefPartialResult4GroupConcatOrderSize is the size of partialResult4GroupConcatOrder
	DefPartialResult4GroupConcatOrderSize = int64(unsafe.Sizeof(partialResult4GroupConcatOrder{}))
	// DefPartialResult4GroupConcatOrderDistinctSize is the size of partialResult4GroupConcatOrderDistinct
	DefPartialResult4GroupConcatOrderDistinctSize = int64(unsafe.Sizeof(partialResult4GroupConcatOrderDistinct{}))

	// DefBytesBufferSize is the size of bytes.Buffer.
	DefBytesBufferSize = int64(unsafe.Sizeof(bytes.Buffer{}))
	// DefTopNRowsSize is the size of topNRows.
	DefTopNRowsSize = int64(unsafe.Sizeof(topNRows{}))
)

type baseGroupConcat4String struct {
	baseAggFunc
	byItems []*util.ByItems

	sep    string
	maxLen uint64
	// According to MySQL, a 'group_concat' function generates exactly one 'truncated' warning during its life time, no matter
	// how many group actually truncated. 'truncated' acts as a sentinel to indicate whether this warning has already been
	// generated.
	truncated *int32
}

func (e *baseGroupConcat4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcat)(pr)
	if p.buffer == nil {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.buffer.String())
	return nil
}

func (e *baseGroupConcat4String) handleTruncateError(sctx sessionctx.Context) (err error) {
	if atomic.CompareAndSwapInt32(e.truncated, 0, 1) {
		if !sctx.GetSessionVars().StmtCtx.TruncateAsWarning {
			return expression.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String())
		}
		sctx.GetSessionVars().StmtCtx.AppendWarning(expression.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String()))
	}
	return nil
}

func (e *baseGroupConcat4String) truncatePartialResultIfNeed(sctx sessionctx.Context, buffer *bytes.Buffer) (err error) {
	if e.maxLen > 0 && uint64(buffer.Len()) > e.maxLen {
		buffer.Truncate(int(e.maxLen))
		return e.handleTruncateError(sctx)
	}
	return nil
}

// nolint:structcheck
type basePartialResult4GroupConcat struct {
	valsBuf *bytes.Buffer
	buffer  *bytes.Buffer
}

type partialResult4GroupConcat struct {
	basePartialResult4GroupConcat
}

type groupConcat struct {
	baseGroupConcat4String
}

func (e *groupConcat) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4GroupConcat)
	p.valsBuf = &bytes.Buffer{}
	return PartialResult(p), DefPartialResult4GroupConcatSize + DefBytesBufferSize
}

func (e *groupConcat) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcat)(pr)
	p.buffer = nil
}

func (e *groupConcat) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcat)(pr)
	v, isNull := "", false
	memDelta += int64(-p.valsBuf.Cap())
	if p.buffer != nil {
		memDelta += int64(-p.buffer.Cap())
	}

	defer func() {
		memDelta += int64(p.valsBuf.Cap())
		if p.buffer != nil {
			memDelta += int64(p.buffer.Cap())
		}
	}()

	for _, row := range rowsInGroup {
		p.valsBuf.Reset()
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				break
			}
			p.valsBuf.WriteString(v)
		}
		if isNull {
			continue
		}
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
			memDelta += DefBytesBufferSize
		} else {
			p.buffer.WriteString(e.sep)
		}
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return memDelta, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return memDelta, nil
}

func (e *groupConcat) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4GroupConcat)(src), (*partialResult4GroupConcat)(dst)
	if p1.buffer == nil {
		return 0, nil
	}
	if p2.buffer == nil {
		p2.buffer = p1.buffer
		return 0, nil
	}
	memDelta -= int64(p2.buffer.Cap())
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	memDelta += int64(p2.buffer.Cap())
	return memDelta, e.truncatePartialResultIfNeed(sctx, p2.buffer)
}

// SetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) GetTruncated() *int32 {
	return e.truncated
}

type partialResult4GroupConcatDistinct struct {
	basePartialResult4GroupConcat
	valSet            set.StringSetWithMemoryUsage
	encodeBytesBuffer []byte
}

type groupConcatDistinct struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4GroupConcatDistinct)
	p.valsBuf = &bytes.Buffer{}
	setSize := int64(0)
	p.valSet, setSize = set.NewStringSetWithMemoryUsage()
	return PartialResult(p), DefPartialResult4GroupConcatDistinctSize + DefBytesBufferSize + setSize
}

func (e *groupConcatDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.buffer = nil
	p.valSet, _ = set.NewStringSetWithMemoryUsage()
}

func (e *groupConcatDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	v, isNull := "", false
	memDelta += int64(-p.valsBuf.Cap()) + (int64(-cap(p.encodeBytesBuffer)))
	if p.buffer != nil {
		memDelta += int64(-p.buffer.Cap())
	}
	defer func() {
		memDelta += int64(p.valsBuf.Cap()) + (int64(cap(p.encodeBytesBuffer)))
		if p.buffer != nil {
			memDelta += int64(p.buffer.Cap())
		}
	}()

	collators := make([]collate.Collator, 0, len(e.args))
	for _, arg := range e.args {
		collators = append(collators, collate.GetCollator(arg.GetType().GetCollate()))
	}

	for _, row := range rowsInGroup {
		p.valsBuf.Reset()
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for i, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, collators[i].Key(v))
			p.valsBuf.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			continue
		}
		memDelta += p.valSet.Insert(joinedVal)
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
			memDelta += DefBytesBufferSize
		} else {
			p.buffer.WriteString(e.sep)
		}
		// write values
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return memDelta, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return memDelta, nil
}

// SetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) GetTruncated() *int32 {
	return e.truncated
}

type sortRow struct {
	buffer  *bytes.Buffer
	byItems []*types.Datum
}

type topNRows struct {
	rows []sortRow
	desc []bool
	sctx sessionctx.Context
	err  error

	currSize  uint64
	limitSize uint64
	sepSize   uint64
	// If sep is truncated, we need to append part of sep to result.
	// In the following example, session.group_concat_max_len is 10 and sep is '---'.
	// ('---', 'ccc') should be poped from heap, so '-' should be appended to result.
	// eg: 'aaa---bbb---ccc' -> 'aaa---bbb-'
	isSepTruncated bool
	collators      []collate.Collator
}

func (h topNRows) Len() int {
	return len(h.rows)
}

func (h topNRows) Less(i, j int) bool {
	n := len(h.rows[i].byItems)
	for k := 0; k < n; k++ {
		ret, err := h.rows[i].byItems[k].Compare(h.sctx.GetSessionVars().StmtCtx, h.rows[j].byItems[k], h.collators[k])
		if err != nil {
			h.err = err
			return false
		}
		if h.desc[k] {
			ret = -ret
		}
		if ret > 0 {
			return true
		}
		if ret < 0 {
			return false
		}
	}
	return false
}

func (h topNRows) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *topNRows) Push(x interface{}) {
	h.rows = append(h.rows, x.(sortRow))
}

func (h *topNRows) Pop() interface{} {
	n := len(h.rows)
	x := h.rows[n-1]
	h.rows = h.rows[:n-1]
	return x
}

func (h *topNRows) tryToAdd(row sortRow) (truncated bool, memDelta int64) {
	h.currSize += uint64(row.buffer.Len())
	if len(h.rows) > 0 {
		h.currSize += h.sepSize
	}
	heap.Push(h, row)
	memDelta += int64(row.buffer.Cap())
	for _, dt := range row.byItems {
		memDelta += GetDatumMemSize(dt)
	}
	if h.currSize <= h.limitSize {
		return false, memDelta
	}

	for h.currSize > h.limitSize {
		debt := h.currSize - h.limitSize
		if uint64(h.rows[0].buffer.Len()) > debt {
			h.currSize -= debt
			h.rows[0].buffer.Truncate(h.rows[0].buffer.Len() - int(debt))
		} else {
			h.currSize -= uint64(h.rows[0].buffer.Len()) + h.sepSize
			memDelta -= int64(h.rows[0].buffer.Cap())
			for _, dt := range h.rows[0].byItems {
				memDelta -= GetDatumMemSize(dt)
			}
			heap.Pop(h)
			h.isSepTruncated = true
		}
	}
	return true, memDelta
}

func (h *topNRows) reset() {
	h.rows = h.rows[:0]
	h.err = nil
	h.currSize = 0
}

func (h *topNRows) concat(sep string, truncated bool) string {
	buffer := new(bytes.Buffer)
	sort.Sort(sort.Reverse(h))
	for i, row := range h.rows {
		if i != 0 {
			buffer.WriteString(sep)
		}
		buffer.Write(row.buffer.Bytes())
	}
	if h.isSepTruncated {
		buffer.WriteString(sep)
		if uint64(buffer.Len()) > h.limitSize {
			buffer.Truncate(int(h.limitSize))
		}
	}
	return buffer.String()
}

type partialResult4GroupConcatOrder struct {
	topN *topNRows
}

type groupConcatOrder struct {
	baseGroupConcat4String
}

func (e *groupConcatOrder) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcatOrder)(pr)
	if p.topN.Len() == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.topN.concat(e.sep, *e.truncated == 1))
	return nil
}

func (e *groupConcatOrder) AllocPartialResult() (pr PartialResult, memDelta int64) {
	desc := make([]bool, len(e.byItems))
	ctors := make([]collate.Collator, 0, len(e.byItems))
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
		ctors = append(ctors, collate.GetCollator(byItem.Expr.GetType().GetCollate()))
	}
	p := &partialResult4GroupConcatOrder{
		topN: &topNRows{
			desc:           desc,
			currSize:       0,
			limitSize:      e.maxLen,
			sepSize:        uint64(len(e.sep)),
			isSepTruncated: false,
			collators:      ctors,
		},
	}
	return PartialResult(p), DefPartialResult4GroupConcatOrderSize + DefTopNRowsSize
}

func (e *groupConcatOrder) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatOrder)(pr)
	p.topN.reset()
}

func (e *groupConcatOrder) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatOrder)(pr)
	p.topN.sctx = sctx
	v, isNull := "", false
	for _, row := range rowsInGroup {
		buffer := new(bytes.Buffer)
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				break
			}
			buffer.WriteString(v)
		}
		if isNull {
			continue
		}
		sortRow := sortRow{
			buffer:  buffer,
			byItems: make([]*types.Datum, 0, len(e.byItems)),
		}
		for _, byItem := range e.byItems {
			d, err := byItem.Expr.Eval(row)
			if err != nil {
				return memDelta, err
			}
			sortRow.byItems = append(sortRow.byItems, d.Clone())
		}
		truncated, sortRowMemSize := p.topN.tryToAdd(sortRow)
		memDelta += sortRowMemSize
		if p.topN.err != nil {
			return memDelta, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return memDelta, err
			}
		}
	}
	return memDelta, nil
}

func (e *groupConcatOrder) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in executorBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, plannercore.ErrInternal.GenWithStack("groupConcatOrder.MergePartialResult should not be called")
}

// SetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatOrder) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatOrder) GetTruncated() *int32 {
	return e.truncated
}

type partialResult4GroupConcatOrderDistinct struct {
	topN              *topNRows
	valSet            set.StringSetWithMemoryUsage
	encodeBytesBuffer []byte
}

type groupConcatDistinctOrder struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinctOrder) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	if p.topN.Len() == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.topN.concat(e.sep, *e.truncated == 1))
	return nil
}

func (e *groupConcatDistinctOrder) AllocPartialResult() (pr PartialResult, memDelta int64) {
	desc := make([]bool, len(e.byItems))
	ctors := make([]collate.Collator, 0, len(e.byItems))
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
		ctors = append(ctors, collate.GetCollator(byItem.Expr.GetType().GetCollate()))
	}
	valSet, setSize := set.NewStringSetWithMemoryUsage()
	p := &partialResult4GroupConcatOrderDistinct{
		topN: &topNRows{
			desc:           desc,
			currSize:       0,
			limitSize:      e.maxLen,
			sepSize:        uint64(len(e.sep)),
			isSepTruncated: false,
			collators:      ctors,
		},
		valSet: valSet,
	}
	return PartialResult(p), DefPartialResult4GroupConcatOrderDistinctSize + DefTopNRowsSize + setSize
}

func (e *groupConcatDistinctOrder) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.reset()
	p.valSet, _ = set.NewStringSetWithMemoryUsage()
}

func (e *groupConcatDistinctOrder) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.sctx = sctx
	v, isNull := "", false
	memDelta -= int64(cap(p.encodeBytesBuffer))
	defer func() { memDelta += int64(cap(p.encodeBytesBuffer)) }()

	collators := make([]collate.Collator, 0, len(e.args))
	for _, arg := range e.args {
		collators = append(collators, collate.GetCollator(arg.GetType().GetCollate()))
	}

	for _, row := range rowsInGroup {
		buffer := new(bytes.Buffer)
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for i, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, collators[i].Key(v))
			buffer.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			continue
		}
		memDelta += p.valSet.Insert(joinedVal)
		sortRow := sortRow{
			buffer:  buffer,
			byItems: make([]*types.Datum, 0, len(e.byItems)),
		}
		for _, byItem := range e.byItems {
			d, err := byItem.Expr.Eval(row)
			if err != nil {
				return memDelta, err
			}
			sortRow.byItems = append(sortRow.byItems, d.Clone())
		}
		truncated, sortRowMemSize := p.topN.tryToAdd(sortRow)
		memDelta += sortRowMemSize
		if p.topN.err != nil {
			return memDelta, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return memDelta, err
			}
		}
	}
	return memDelta, nil
}

func (e *groupConcatDistinctOrder) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in executorBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, plannercore.ErrInternal.GenWithStack("groupConcatDistinctOrder.MergePartialResult should not be called")
}

// GetDatumMemSize calculates the memory size of each types.Datum in sortRow.byItems.
// types.Datum memory size = variable type's memory size + variable value's memory size.
func GetDatumMemSize(d *types.Datum) int64 {
	var datumMemSize int64
	datumMemSize += int64(unsafe.Sizeof(*d))
	datumMemSize += int64(len(d.Collation()))
	datumMemSize += int64(len(d.GetBytes()))
	datumMemSize += getValMemDelta(d.GetInterface()) - DefInterfaceSize
	return datumMemSize
}
