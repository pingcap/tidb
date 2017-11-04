// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &HashAggExec{}

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in AggFuncs.
type HashAggExec struct {
	baseExecutor

	executed      bool
	hasGby        bool
	aggType       plan.AggregationType
	sc            *variable.StatementContext
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
func (e *HashAggExec) Open() error {
	e.executed = false
	e.groupMap = mvmap.NewMVMap()
	e.groupIterator = e.groupMap.NewIterator()
	e.aggCtxsMap = make(aggCtxsMapper, 0)
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next() (Row, error) {
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
	retRow := make([]types.Datum, 0, len(e.AggFuncs))
	aggCtxs := e.getContexts(groupKey)
	for i, af := range e.AggFuncs {
		retRow = append(retRow, af.GetResult(aggCtxs[i]))
	}
	return retRow, nil
}

func (e *HashAggExec) getGroupKey(row Row) ([]byte, error) {
	if !e.hasGby {
		return []byte{}, nil
	}
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
