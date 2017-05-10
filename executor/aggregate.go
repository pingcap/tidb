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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/types"
)

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in AggFuncs.
type HashAggExec struct {
	baseExecutor

	executed      bool
	hasGby        bool
	aggType       plan.AggregationType
	sc            *variable.StatementContext
	AggFuncs      []expression.AggregationFunction
	groupMap      *mvmap.MVMap
	groupIterator *mvmap.Iterator
	GroupByItems  []expression.Expression
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	e.groupMap = nil
	e.groupIterator = nil
	for _, agg := range e.AggFuncs {
		agg.Reset()
	}
	return errors.Trace(e.children[0].Close())
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open() error {
	e.executed = false
	e.groupMap = mvmap.NewMVMap()
	e.groupIterator = e.groupMap.NewIterator()
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next() (*Row, error) {
	// In this stage we consider all data from src as a single group.
	if !e.executed {
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		if (e.groupMap.Len() == 0) && !e.hasGby {
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
	retRow := &Row{Data: make([]types.Datum, 0, len(e.AggFuncs))}
	for _, af := range e.AggFuncs {
		retRow.Data = append(retRow.Data, af.GetGroupResult(groupKey))
	}
	return retRow, nil
}

func (e *HashAggExec) getGroupKey(row *Row) ([]byte, error) {
	if e.aggType == plan.FinalAgg {
		val, err := e.GroupByItems[0].Eval(row.Data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return val.GetBytes(), nil
	}
	if !e.hasGby {
		return []byte{}, nil
	}
	vals := make([]types.Datum, 0, len(e.GroupByItems))
	for _, item := range e.GroupByItems {
		v, err := item.Eval(row.Data)
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
func (e *HashAggExec) innerNext() (ret bool, err error) {
	srcRow, err := e.children[0].Next()
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
	for _, af := range e.AggFuncs {
		af.Update(srcRow.Data, groupKey, e.sc)
	}
	return true, nil
}

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed           bool
	hasData            bool
	StmtCtx            *variable.StatementContext
	AggFuncs           []expression.AggregationFunction
	GroupByItems       []expression.Expression
	curGroupEncodedKey []byte
	curGroupKey        []types.Datum
	tmpGroupKey        []types.Datum
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open() error {
	e.executed = false
	e.hasData = false
	for _, agg := range e.AggFuncs {
		agg.Reset()
	}
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next() (*Row, error) {
	if e.executed {
		return nil, nil
	}
	retRow := &Row{Data: make([]types.Datum, 0, len(e.AggFuncs))}
	for {
		row, err := e.children[0].Next()
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
			for _, af := range e.AggFuncs {
				retRow.Data = append(retRow.Data, af.GetStreamResult())
			}
		}
		if e.executed {
			break
		}
		for _, af := range e.AggFuncs {
			err = af.StreamUpdate(row.Data, e.StmtCtx)
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

// meetNewGroup returns a value that represents if the new group is different from last group.
func (e *StreamAggExec) meetNewGroup(row *Row) (bool, error) {
	if len(e.GroupByItems) == 0 {
		return false, nil
	}
	e.tmpGroupKey = e.tmpGroupKey[:0]
	matched, firstGroup := true, false
	if len(e.curGroupKey) == 0 {
		matched, firstGroup = false, true
	}
	for i, item := range e.GroupByItems {
		v, err := item.Eval(row.Data)
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			c, err := v.CompareDatum(e.StmtCtx, e.curGroupKey[i])
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
	var err error
	e.curGroupEncodedKey, err = codec.EncodeValue(e.curGroupEncodedKey[0:0:cap(e.curGroupEncodedKey)], e.curGroupKey...)
	if err != nil {
		return false, errors.Trace(err)
	}
	return !firstGroup, nil
}
