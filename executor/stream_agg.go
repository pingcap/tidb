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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &StreamAggExec{}

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed           bool
	hasData            bool
	StmtCtx            *variable.StatementContext
	AggFuncs           []aggregation.Aggregation
	aggCtxs            []*aggregation.AggEvaluateContext
	GroupByItems       []expression.Expression
	curGroupEncodedKey []byte
	curGroupKey        []types.Datum
	tmpGroupKey        []types.Datum
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open() error {
	e.executed = false
	e.hasData = false
	e.aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next() (Row, error) {
	if e.executed {
		return nil, nil
	}
	if len(e.aggCtxs) == 0 {
		for _, agg := range e.AggFuncs {
			e.aggCtxs = append(e.aggCtxs, agg.CreateContext())
		}
	}
	retRow := make([]types.Datum, 0, len(e.AggFuncs))
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

// meetNewGroup returns a value that represents if the new group is different from last group.
func (e *StreamAggExec) meetNewGroup(row Row) (bool, error) {
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
	var err error
	e.curGroupEncodedKey, err = codec.EncodeValue(e.curGroupEncodedKey[0:0:cap(e.curGroupEncodedKey)], e.curGroupKey...)
	if err != nil {
		return false, errors.Trace(err)
	}
	return !firstGroup, nil
}
