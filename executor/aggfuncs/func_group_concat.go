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
	"bytes"
	"container/heap"
	"sort"
	"sync/atomic"

	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
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
	return PartialResult(p), 0
}

func (e *groupConcat) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcat)(pr)
	p.buffer = nil
}

func (e *groupConcat) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcat)(pr)
	v, isNull := "", false
	for _, row := range rowsInGroup {
		p.valsBuf.Reset()
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return 0, err
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
		} else {
			p.buffer.WriteString(e.sep)
		}
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return 0, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return 0, nil
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
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	return 0, e.truncatePartialResultIfNeed(sctx, p2.buffer)
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
	valSet            set.StringSet
	encodeBytesBuffer []byte
	needSync          bool
	syncSet           set.SyncSet
}

type groupConcatDistinct struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4GroupConcatDistinct)
	p.valsBuf = &bytes.Buffer{}
	p.valSet = set.NewStringSet()
	return PartialResult(p), 0
}

func (e *groupConcatDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.buffer, p.valSet = nil, set.NewStringSet()
}

func (e *groupConcatDistinct) SetSyncSet(s set.SyncSet, pr PartialResult) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.needSync = true
	p.syncSet = s
}

func (e *groupConcatDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	v, isNull := "", false
	for _, row := range rowsInGroup {
		p.valsBuf.Reset()
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, hack.Slice(v))
			p.valsBuf.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.needSync {
			if p.syncSet.Exist(joinedVal) {
				continue
			}
			p.syncSet.Insert(joinedVal)
		} else {
			if p.valSet.Exist(joinedVal) {
				continue
			}
			p.valSet.Insert(joinedVal)
		}
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(e.sep)
		}
		// write values
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return 0, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return 0, nil
}

func (e *groupConcatDistinct) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4GroupConcatDistinct)(src), (*partialResult4GroupConcatDistinct)(dst)
	if p1.buffer == nil {
		return 0, nil
	}
	if p2.buffer == nil {
		p2.buffer = p1.buffer
		return 0, nil
	}
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	return 0, e.truncatePartialResultIfNeed(sctx, p2.buffer)
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
}

func (h topNRows) Len() int {
	return len(h.rows)
}

func (h topNRows) Less(i, j int) bool {
	n := len(h.rows[i].byItems)
	for k := 0; k < n; k++ {
		ret, err := h.rows[i].byItems[k].CompareDatum(h.sctx.GetSessionVars().StmtCtx, h.rows[j].byItems[k])
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

func (h *topNRows) tryToAdd(row sortRow) (truncated bool) {
	h.currSize += uint64(row.buffer.Len())
	if len(h.rows) > 0 {
		h.currSize += h.sepSize
	}
	heap.Push(h, row)
	if h.currSize <= h.limitSize {
		return false
	}

	for h.currSize > h.limitSize {
		debt := h.currSize - h.limitSize
		if uint64(h.rows[0].buffer.Len()) > debt {
			h.currSize -= debt
			h.rows[0].buffer.Truncate(h.rows[0].buffer.Len() - int(debt))
		} else {
			h.currSize -= uint64(h.rows[0].buffer.Len()) + h.sepSize
			heap.Pop(h)
		}
	}
	return true
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
	if truncated && uint64(buffer.Len()) < h.limitSize {
		// append the last separator, because the last separator may be truncated in tryToAdd.
		buffer.WriteString(sep)
		buffer.Truncate(int(h.limitSize))
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
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
	}
	p := &partialResult4GroupConcatOrder{
		topN: &topNRows{
			desc:      desc,
			currSize:  0,
			limitSize: e.maxLen,
			sepSize:   uint64(len(e.sep)),
		},
	}
	return PartialResult(p), 0
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
				return 0, err
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
				return 0, err
			}
			sortRow.byItems = append(sortRow.byItems, d.Clone())
		}
		truncated := p.topN.tryToAdd(sortRow)
		if p.topN.err != nil {
			return 0, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

func (e *groupConcatOrder) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in executorBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, terror.ClassOptimizer.New(mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal]).GenWithStack("groupConcatOrder.MergePartialResult should not be called")
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
	valSet            set.StringSet
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
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
	}
	p := &partialResult4GroupConcatOrderDistinct{
		topN: &topNRows{
			desc:      desc,
			currSize:  0,
			limitSize: e.maxLen,
			sepSize:   uint64(len(e.sep)),
		},
		valSet: set.NewStringSet(),
	}
	return PartialResult(p), 0
}

func (e *groupConcatDistinctOrder) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.reset()
	p.valSet = set.NewStringSet()
}

func (e *groupConcatDistinctOrder) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.sctx = sctx
	v, isNull := "", false
	for _, row := range rowsInGroup {
		buffer := new(bytes.Buffer)
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, hack.Slice(v))
			buffer.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			continue
		}
		p.valSet.Insert(joinedVal)
		sortRow := sortRow{
			buffer:  buffer,
			byItems: make([]*types.Datum, 0, len(e.byItems)),
		}
		for _, byItem := range e.byItems {
			d, err := byItem.Expr.Eval(row)
			if err != nil {
				return 0, err
			}
			sortRow.byItems = append(sortRow.byItems, d.Clone())
		}
		truncated := p.topN.tryToAdd(sortRow)
		if p.topN.err != nil {
			return 0, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

func (e *groupConcatDistinctOrder) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in executorBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, terror.ClassOptimizer.New(mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal]).GenWithStack("groupConcatDistinctOrder.MergePartialResult should not be called")
}
