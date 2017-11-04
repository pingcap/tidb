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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &SortExec{}

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key []*types.Datum
	row Row
}

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*plan.ByItems
	Rows    []*orderByRow
	Idx     int
	fetched bool
	err     error
	schema  *expression.Schema
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	e.Rows = nil
	return errors.Trace(e.children[0].Close())
}

// Open implements the Executor Open interface.
func (e *SortExec) Open() error {
	e.fetched = false
	e.Idx = 0
	e.Rows = nil
	return errors.Trace(e.children[0].Open())
}

// Len returns the number of rows.
func (e *SortExec) Len() int {
	return len(e.Rows)
}

// Swap implements sort.Interface Swap interface.
func (e *SortExec) Swap(i, j int) {
	e.Rows[i], e.Rows[j] = e.Rows[j], e.Rows[i]
}

// Less implements sort.Interface Less interface.
func (e *SortExec) Less(i, j int) bool {
	sc := e.ctx.GetSessionVars().StmtCtx
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(sc, v2)
		if err != nil {
			e.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// Next implements the Executor Next interface.
func (e *SortExec) Next() (Row, error) {
	if !e.fetched {
		for {
			srcRow, err := e.children[0].Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]*types.Datum, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				key, err := byItem.Expr.Eval(srcRow)
				if err != nil {
					return nil, errors.Trace(err)
				}
				orderRow.key[i] = &key
			}
			e.Rows = append(e.Rows, orderRow)
		}
		sort.Sort(e)
		e.fetched = true
	}
	if e.err != nil {
		return nil, errors.Trace(e.err)
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}
