// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	"golang.org/x/net/context"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext


// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed     bool
	hasData      bool
	StmtCtx      *stmtctx.StatementContext
	AggFuncs     []aggregation.Aggregation
	aggCtxs      []*aggregation.AggEvaluateContext
	GroupByItems []expression.Expression
	curGroupKey  []types.Datum
	tmpGroupKey  []types.Datum

	// for chunk execution.
	inputIter  *chunk.Iterator4Chunk
	inputRow   chunk.Row
	mutableRow chunk.MutRow
	rowBuffer  []types.Datum
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.executed = false
	e.hasData = false
	e.inputIter = chunk.NewIterator4Chunk(e.childrenResults[0])
	e.inputRow = e.inputIter.End()
	e.mutableRow = chunk.MutRowFromTypes(e.retTypes())
	e.rowBuffer = make([]types.Datum, 0, e.Schema().Len())

	e.aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
	for _, agg := range e.AggFuncs {
		e.aggCtxs = append(e.aggCtxs, agg.CreateContext(e.ctx.GetSessionVars().StmtCtx))
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(ctx context.Context) (Row, error) {
	if e.executed {
		return nil, nil
	}
	retRow := make([]types.Datum, 0, len(e.AggFuncs))
	for {
		row, err := e.children[0].Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var newGroup bool
		if row == nil {
			newGroup = true
			e.executed = true
		} else {
			e.hasData = true
			newGroup, err = e.meetNewGroup(row)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if newGroup {
			for i, af := range e.AggFuncs {
				retRow = append(retRow, af.GetResult(e.aggCtxs[i]))
				// Clear stream results after grabbing them.
				e.aggCtxs[i] = af.CreateContext(e.ctx.GetSessionVars().StmtCtx)
			}
		}
		if e.executed {
			break
		}
		for i, af := range e.AggFuncs {
			err = af.Update(e.aggCtxs[i], e.StmtCtx, row)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if newGroup {
			break
		}
	}
	if !e.hasData && len(e.GroupByItems) > 0 {
		return nil, nil
	}
	return retRow, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *StreamAggExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	for !e.executed && chk.NumRows() < e.maxChunkSize {
		err := e.consumeOneGroup(ctx, chk)
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *StreamAggExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	for !e.executed {
		if err := e.fetchChildIfNecessary(ctx, chk); err != nil {
			return errors.Trace(err)
		}
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			meetNewGroup, err := e.meetNewGroup(e.inputRow)
			if err != nil {
				return errors.Trace(err)
			}
			if meetNewGroup {
				e.appendResult2Chunk(chk)
			}
			for i, af := range e.AggFuncs {
				err := af.Update(e.aggCtxs[i], e.StmtCtx, e.inputRow)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if meetNewGroup {
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
	}
	return nil
}

func (e *StreamAggExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) error {
	if e.inputRow != e.inputIter.End() {
		return nil
	}

	err := e.children[0].NextChunk(ctx, e.childrenResults[0])
	if err != nil {
		return errors.Trace(err)
	}
	// No more data.
	if e.childrenResults[0].NumRows() == 0 {
		if e.hasData || len(e.GroupByItems) == 0 {
			e.appendResult2Chunk(chk)
		}
		e.executed = true
		return nil
	}

	// Reach here, "e.childrenResults[0].NumRows() > 0" is guaranteed.
	e.inputRow = e.inputIter.Begin()
	e.hasData = true
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and realloc the evaluation context for each aggregation.
func (e *StreamAggExec) appendResult2Chunk(chk *chunk.Chunk) {
	e.rowBuffer = e.rowBuffer[:0]
	for i, af := range e.AggFuncs {
		e.rowBuffer = append(e.rowBuffer, af.GetResult(e.aggCtxs[i]))
		e.aggCtxs[i] = af.CreateContext(e.ctx.GetSessionVars().StmtCtx)
	}
	e.mutableRow.SetDatums(e.rowBuffer...)
	chk.AppendRow(e.mutableRow.ToRow())
}

// meetNewGroup returns a value that represents if the new group is different from last group.
func (e *StreamAggExec) meetNewGroup(row types.Row) (bool, error) {
	if len(e.GroupByItems) == 0 {
		return false, nil
	}
	e.tmpGroupKey = e.tmpGroupKey[:0]
	matched, firstGroup := true, false
	if len(e.curGroupKey) == 0 {
		matched, firstGroup = false, true
	}
	for i, item := range e.GroupByItems {
		v, err := item.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			c, err := v.CompareDatum(e.StmtCtx, &e.curGroupKey[i])
			if err != nil {
				return false, errors.Trace(err)
			}
			matched = c == 0
		}
		e.tmpGroupKey = append(e.tmpGroupKey, v)
	}
	if matched {
		return false, nil
	}
	e.curGroupKey = e.curGroupKey[:0]
	for _, v := range e.tmpGroupKey {
		e.curGroupKey = append(e.curGroupKey, *((&v).Copy()))
	}
	return !firstGroup, nil
}
