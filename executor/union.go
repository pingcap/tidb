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
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
)

var _ Executor = &UnionExec{}

// UnionExec represents union executor.
// UnionExec has multiple source Executors, it executes them sequentially, and do conversion to the same type
// as source Executors may has different field type, we need to do conversion.
type UnionExec struct {
	baseExecutor

	finished atomic.Value
	resultCh chan *execResult
	rows     []Row
	cursor   int
	wg       sync.WaitGroup
	closedCh chan struct{}
}

type execResult struct {
	rows []Row
	err  error
}

// Schema implements the Executor Schema interface.
func (e *UnionExec) Schema() *expression.Schema {
	return e.schema
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultCh)
	close(e.closedCh)
}

func (e *UnionExec) fetchData(idx int) {
	defer e.wg.Done()
	for {
		result := &execResult{
			rows: make([]Row, 0, batchSize),
			err:  nil,
		}
		for i := 0; i < batchSize; i++ {
			if e.finished.Load().(bool) {
				return
			}
			row, err := e.children[idx].Next()
			if err != nil {
				e.finished.Store(true)
				result.err = err
				e.resultCh <- result
				return
			}
			if row == nil {
				if len(result.rows) > 0 {
					e.resultCh <- result
				}
				return
			}
			// TODO: Add cast function in plan building phase.
			for j := range row {
				col := e.schema.Columns[j]
				val, err := row[j].ConvertTo(e.ctx.GetSessionVars().StmtCtx, col.RetType)
				if err != nil {
					e.finished.Store(true)
					result.err = err
					e.resultCh <- result
					return
				}
				row[j] = val
			}
			result.rows = append(result.rows, row)
		}
		e.resultCh <- result
	}
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open() error {
	e.finished.Store(false)
	e.resultCh = make(chan *execResult, len(e.children))
	e.closedCh = make(chan struct{})
	e.cursor = 0
	var err error
	for i, child := range e.children {
		err = child.Open()
		if err != nil {
			break
		}
		e.wg.Add(1)
		go e.fetchData(i)
	}
	go e.waitAllFinished()
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next() (Row, error) {
	if e.cursor >= len(e.rows) {
		result, ok := <-e.resultCh
		if !ok {
			return nil, nil
		}
		if result.err != nil {
			return nil, errors.Trace(result.err)
		}
		if len(result.rows) == 0 {
			return nil, nil
		}
		e.rows = result.rows
		e.cursor = 0
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	e.finished.Store(true)
	<-e.closedCh
	e.rows = nil
	return errors.Trace(e.baseExecutor.Close())
}
