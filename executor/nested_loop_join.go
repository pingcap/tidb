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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

// NestedLoopJoinExec implements nested-loop algorithm for join.
type NestedLoopJoinExec struct {
	innerRows     []Row
	cursor        int
	resultRows    []Row
	SmallExec     Executor
	BigExec       Executor
	leftSmall     bool
	prepared      bool
	Ctx           context.Context
	SmallFilter   expression.CNFExprs
	BigFilter     expression.CNFExprs
	OtherFilter   expression.CNFExprs
	schema        *expression.Schema
	outer         bool
	defaultValues []types.Datum
}

// Schema implements Executor interface.
func (e *NestedLoopJoinExec) Schema() *expression.Schema {
	return e.schema
}

// Close implements Executor interface.
func (e *NestedLoopJoinExec) Close() error {
	e.resultRows = nil
	e.innerRows = nil
	return e.BigExec.Close()
}

// Open implements Executor Open interface.
func (e *NestedLoopJoinExec) Open() error {
	e.cursor = 0
	e.prepared = false
	e.resultRows = e.resultRows[:0]
	e.innerRows = e.innerRows[:0]
	return errors.Trace(e.BigExec.Open())
}

func (e *NestedLoopJoinExec) fetchBigRow() (Row, bool, error) {
	for {
		bigRow, err := e.BigExec.Next()
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if bigRow == nil {
			return nil, false, e.BigExec.Close()
		}

		matched, err := expression.EvalBool(e.BigFilter, bigRow, e.Ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			return bigRow, true, nil
		} else if e.outer {
			return bigRow, false, nil
		}
	}
}

// prepare runs the first time when 'Next' is called and it reads all data from the small table and stores
// them in a slice.
func (e *NestedLoopJoinExec) prepare() error {
	err := e.SmallExec.Open()
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(e.SmallExec.Close)
	e.innerRows = e.innerRows[:0]
	e.prepared = true
	for {
		row, err := e.SmallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}

		matched, err := expression.EvalBool(e.SmallFilter, row, e.Ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			e.innerRows = append(e.innerRows, row)
		}
	}
}

func (e *NestedLoopJoinExec) fillRowWithDefaultValue(bigRow Row) (returnRow Row) {
	smallRow := make([]types.Datum, e.SmallExec.Schema().Len())
	copy(smallRow, e.defaultValues)
	if e.leftSmall {
		returnRow = makeJoinRow(smallRow, bigRow)
	} else {
		returnRow = makeJoinRow(bigRow, smallRow)
	}
	return returnRow
}

func (e *NestedLoopJoinExec) doJoin(bigRow Row, match bool) ([]Row, error) {
	e.resultRows = e.resultRows[0:0]
	if !match && e.outer {
		row := e.fillRowWithDefaultValue(bigRow)
		e.resultRows = append(e.resultRows, row)
		return e.resultRows, nil
	}
	for _, row := range e.innerRows {
		var mergedRow Row
		if e.leftSmall {
			mergedRow = makeJoinRow(row, bigRow)
		} else {
			mergedRow = makeJoinRow(bigRow, row)
		}
		matched, err := expression.EvalBool(e.OtherFilter, mergedRow, e.Ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !matched {
			continue
		}
		e.resultRows = append(e.resultRows, mergedRow)
	}
	if len(e.resultRows) == 0 && e.outer {
		e.resultRows = append(e.resultRows, e.fillRowWithDefaultValue(bigRow))
	}
	return e.resultRows, nil
}

// Next implements the Executor interface.
func (e *NestedLoopJoinExec) Next() (Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for {
		if e.cursor < len(e.resultRows) {
			retRow := e.resultRows[e.cursor]
			e.cursor++
			return retRow, nil
		}
		bigRow, match, err := e.fetchBigRow()
		if bigRow == nil || err != nil {
			return bigRow, errors.Trace(err)
		}
		e.resultRows, err = e.doJoin(bigRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.cursor = 0
	}
}
