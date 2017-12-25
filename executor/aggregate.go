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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	goctx "golang.org/x/net/context"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in AggFuncs.
type HashAggExec struct {
	baseExecutor

	executed      bool
	sc            *stmtctx.StatementContext
	AggFuncs      []aggregation.Aggregation
	aggCtxsMap    aggCtxsMapper
	groupMap      *mvmap.MVMap
	groupIterator *mvmap.Iterator
	GroupByItems  []expression.Expression
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	e.groupMap = nil
	e.groupIterator = nil
	e.aggCtxsMap = nil
	return errors.Trace(e.children[0].Close())
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open(goCtx goctx.Context) error {
	e.executed = false
	e.groupMap = mvmap.NewMVMap()
	e.groupIterator = e.groupMap.NewIterator()
	e.aggCtxsMap = make(aggCtxsMapper, 0)
	return errors.Trace(e.children[0].Open(goCtx))
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next(goCtx goctx.Context) (Row, error) {
	// In this stage we consider all data from src as a single group.
	if !e.executed {
		for {
			hasMore, err := e.innerNext(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		if (e.groupMap.Len() == 0) && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groupMap.Put([]byte{}, []byte{})
		}
		e.executed = true
	}
	groupKey, _ := e.groupIterator.Next()
	if groupKey == nil {
		return nil, nil
	}
	retRow := make([]types.Datum, 0, len(e.AggFuncs))
	aggCtxs := e.getContexts(groupKey)
	for i, af := range e.AggFuncs {
		retRow = append(retRow, af.GetResult(aggCtxs[i]))
	}
	return retRow, nil
}

func (e *HashAggExec) getGroupKey(row Row) ([]byte, error) {
	vals := make([]types.Datum, 0, len(e.GroupByItems))
	for _, item := range e.GroupByItems {
		v, err := item.Eval(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	bs, err := codec.EncodeValue([]byte{}, vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return bs, nil
}

// innerNext fetches a single row from src and update each aggregate function.
// If the first return value is false, it means there is no more data from src.
func (e *HashAggExec) innerNext(goCtx goctx.Context) (ret bool, err error) {
	srcRow, err := e.children[0].Next(goCtx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if srcRow == nil {
		return false, nil
	}
	e.executed = true
	groupKey, err := e.getGroupKey(srcRow)
	if err != nil {
		return false, errors.Trace(err)
	}
	if e.groupMap.Get(groupKey) == nil {
		e.groupMap.Put(groupKey, []byte{})
	}
	aggCtxs := e.getContexts(groupKey)
	for i, af := range e.AggFuncs {
		err = af.Update(aggCtxs[i], e.sc, srcRow)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	return true, nil
}

func (e *HashAggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
		for _, af := range e.AggFuncs {
			aggCtxs = append(aggCtxs, af.CreateContext())
		}
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}

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
	childCursor int
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.executed = false
	e.hasData = false
	e.aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
	for _, agg := range e.AggFuncs {
		e.aggCtxs = append(e.aggCtxs, agg.CreateContext())
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(goCtx goctx.Context) (Row, error) {
	if e.executed {
		return nil, nil
	}
	retRow := make([]types.Datum, 0, len(e.AggFuncs))
	for {
		row, err := e.children[0].Next(goCtx)
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
				e.aggCtxs[i] = af.CreateContext()
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
func (e *StreamAggExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()

	if e.executed {
		return nil
	}

	mutableRow := chunk.MutRowFromTypes(e.Schema().GetTypes())
	retRow := make([]types.Datum, 0, e.Schema().Len())

	for {
		if e.childCursor < e.childrenResults[0].NumRows() {
			for row := e.childrenResults[0].GetRow(e.childCursor); row != e.childrenResults[0].End(); row = row.Next() {
				meetNewGroup, err := e.meetNewGroup(row)
				if err != nil {
					e.executed = true
					return errors.Trace(err)
				}
				if meetNewGroup {
					retRow = retRow[:0]
					for i, af := range e.AggFuncs {
						retRow = append(retRow, af.GetResult(e.aggCtxs[i]))
						e.aggCtxs[i] = af.CreateContext()
					}
					mutableRow.SetDatums(retRow...)
					if chk.NumCols() == 0 {
						chk.SetNumVirtualRows(chk.NumRows() + 1)
					} else {
						chk.AppendRow(0, mutableRow.ToRow())
					}
				}
				for i, af := range e.AggFuncs {
					err := af.Update(e.aggCtxs[i], e.StmtCtx, row)
					if err != nil {
						e.executed = true
						return errors.Trace(err)
					}
				}
				if meetNewGroup && chk.NumRows() >= e.maxChunkSize {
					e.childCursor = row.Idx() + 1
					return nil
				}
			}
		}

		// Get resource rows from child.
		err := e.children[0].NextChunk(goCtx, e.childrenResults[0])
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
		e.childCursor = 0

		// No more data.
		if e.childrenResults[0].NumRows() == 0 {
			e.executed = true
			if !e.hasData && len(e.GroupByItems) != 0 {
				return nil
			}
			retRow = retRow[:0]
			for i, af := range e.AggFuncs {
				retRow = append(retRow, af.GetResult(e.aggCtxs[i]))
				e.aggCtxs[i] = af.CreateContext()
			}
			mutableRow.SetDatums(retRow...)
			if chk.NumCols() == 0 {
				chk.SetNumVirtualRows(chk.NumRows() + 1)
			} else {
				chk.AppendRow(0, mutableRow.ToRow())
			}
			return nil
		}
		e.hasData = true
	}
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
	e.curGroupKey = e.tmpGroupKey
	return !firstGroup, nil
}
