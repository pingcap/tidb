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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &XAggregateExec{}
)

// finalAggregater is used to merge all partial aggregate results from multiple regions and get final result.
type finalAggregater struct {
	name         string
	currentGroup []byte
	// contextPerGroupMap is used to store aggregate evaluation context.
	// Each entry for a group.
	contextPerGroupMap map[string](*ast.AggEvaluateContext)
}

// Update is used for update aggregate context.
func (n *finalAggregater) update(count uint64, value types.Datum) error {
	switch n.name {
	case ast.AggFuncCount:
		return n.updateCount(count)
	}
	return nil
}

// GetContext gets aggregate evaluation context for the current group.
// If it is nil, add a new context into contextPerGroupMap.
func (n *finalAggregater) getContext() *ast.AggEvaluateContext {
	if n.contextPerGroupMap == nil {
		n.contextPerGroupMap = make(map[string](*ast.AggEvaluateContext))
	}
	if _, ok := n.contextPerGroupMap[string(n.currentGroup)]; !ok {
		n.contextPerGroupMap[string(n.currentGroup)] = &ast.AggEvaluateContext{}
	}
	return n.contextPerGroupMap[string(n.currentGroup)]
}

func (n *finalAggregater) updateCount(count uint64) error {
	ctx := n.getContext()
	ctx.Count += int64(count)
	return nil
}

// XAggregateExec deals with all the aggregate functions.
// It is built from Aggregate Plan. When Next() is called, it reads all the data from Src and updates all the items in AggFuncs.
// TODO: Support having.
type XAggregateExec struct {
	Src               Executor
	ResultFields      []*ast.ResultField
	AggFields         []*types.FieldType
	executed          bool
	ctx               context.Context
	AggFuncs          []*ast.AggregateFuncExpr
	aggregaters       []*finalAggregater
	groupMap          map[string]bool
	groups            []string
	hasGroupBy        bool
	currentGroupIndex int
}

// Fields implements Executor Fields interface.
func (e *XAggregateExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
func (e *XAggregateExec) Next() (*Row, error) {
	// In this stage we consider all data from src as a single group.
	if !e.executed {
		e.groupMap = make(map[string]bool)
		e.groups = []string{}
		e.aggregaters = make([]*finalAggregater, len(e.AggFuncs))
		for i, af := range e.AggFuncs {
			agg := &finalAggregater{
				name: strings.ToLower(af.F),
			}
			e.aggregaters[i] = agg
		}
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
		if len(e.groups) == 0 && !e.hasGroupBy {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groups = append(e.groups, singleGroup)
		}
		// Set AggregateFuncExprs' context.
		for i, agg := range e.aggregaters {
			e.AggFuncs[i].SetContext(agg.contextPerGroupMap)
		}
	}
	if e.currentGroupIndex >= len(e.groups) {
		return nil, nil
	}
	groupKey := e.groups[e.currentGroupIndex]
	for _, af := range e.AggFuncs {
		af.CurrentGroup = groupKey
	}
	e.currentGroupIndex++
	return &Row{}, nil
}

// Fetch a single row from src and update each aggregate function.
// If the first return value is false, it means there is no more data from src.
func (e *XAggregateExec) innerNext() (bool, error) {
	var row *Row
	if e.Src != nil {
		var err error
		row, err = e.Src.Next()
		if err != nil {
			return false, errors.Trace(err)
		}
		if row == nil {
			return false, nil
		}
	} else {
		// If Src is nil, only one row should be returned.
		if e.executed {
			return false, nil
		}
	}
	e.executed = true
	groupKey := row.Data[0].GetBytes()
	if _, ok := e.groupMap[string(groupKey)]; !ok {
		e.groupMap[string(groupKey)] = true
		e.groups = append(e.groups, string(groupKey))
	}
	for i, agg := range e.aggregaters {
		count := row.Data[2*i+1].GetUint64()
		value := row.Data[2*i+2]
		agg.currentGroup = groupKey
		agg.update(count, value)
	}
	return true, nil
}

// Close implements Executor Close interface.
func (e *XAggregateExec) Close() error {
	for _, af := range e.AggFuncs {
		af.Clear()
	}
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
}
