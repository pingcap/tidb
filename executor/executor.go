// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
)

var (
	_ Executor = &CheckTableExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}
)

// Row represents a result set row, it may be returned from a table, a join, or a projection.
//
// The following cases will need store the handle information:
//
// If the top plan is update or delete, then every executor will need the handle.
// If there is an union scan, then the below scan plan must store the handle.
// If there is sort need in the double read, then the table scan of the double read must store the handle.
// If there is a select for update. then we need to store the handle until the lock plan. But if there is aggregation, the handle info can be removed.
// Otherwise the executor's returned rows don't need to store the handle information.
type Row = types.DatumRow

type baseExecutor struct {
	children []Executor
	ctx      context.Context
	schema   *expression.Schema
}

// Open implements the Executor Open interface.
func (e *baseExecutor) Open() error {
	for _, child := range e.children {
		err := child.Open()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *baseExecutor) Close() error {
	for _, child := range e.children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Schema implements the Executor Schema interface.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

func newBaseExecutor(schema *expression.Schema, ctx context.Context, children ...Executor) baseExecutor {
	return baseExecutor{
		children: children,
		ctx:      ctx,
		schema:   schema,
	}
}

// Executor executes a query.
type Executor interface {
	Next() (Row, error)
	Close() error
	Open() error
	Schema() *expression.Schema
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	JobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next() (Row, error) {
	var row Row
	if e.cursor < len(e.JobIDs) {
		ret := "successful"
		if e.errs[e.cursor] != nil {
			ret = fmt.Sprintf("error: %v", e.errs[e.cursor])
		}
		row = types.MakeDatums(e.JobIDs[e.cursor], ret)
		e.cursor++
	}

	return row, nil
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

	tables []*ast.TableName
	ctx    context.Context
	done   bool
	is     infoschema.InfoSchema
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next() (Row, error) {
	if e.done {
		return nil, nil
	}

	dbName := model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)

	for _, t := range e.tables {
		tb, err := e.is.TableByName(dbName, t.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, idx := range tb.Indices() {
			txn := e.ctx.Txn()
			err = admin.CompareIndexData(txn, tb, idx)
			if err != nil {
				return nil, errors.Errorf("%v err:%v", t.Name, err)
			}
		}
	}
	e.done = true

	return nil, nil
}

// Close implements plan.Plan Close interface.
func (e *CheckTableExec) Close() error {
	return nil
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) (rows [][]types.Datum, err error) {
		err = ctx.ActivePendingTxn()
		if err != nil {
			return rows, errors.Trace(err)
		}
		e := &executorBuilder{is: is, ctx: ctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, errors.Trace(err)
		}
		err = exec.Open()
		if err != nil {
			return rows, errors.Trace(err)
		}
		for {
			row, err := exec.Next()
			if err != nil {
				return rows, errors.Trace(err)
			}
			if row == nil {
				return rows, errors.Trace(exec.Close())
			}
			rows = append(rows, row)
		}
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	rowCount  int
	returnCnt int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open() error {
	e.returnCnt = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next() (Row, error) {
	if e.returnCnt >= e.rowCount {
		return nil, nil
	}
	e.returnCnt++
	return Row{}, nil
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t          table.Table
	asName     *model.CIStr
	ctx        context.Context
	ranges     []types.IntColumnRange
	seekHandle int64
	iter       kv.Iterator
	cursor     int
	schema     *expression.Schema
	columns    []*model.ColumnInfo

	isVirtualTable     bool
	virtualTableRows   [][]types.Datum
	virtualTableCursor int
}

// Schema implements the Executor Schema interface.
func (e *TableScanExec) Schema() *expression.Schema {
	return e.schema
}

// Next implements the Executor interface.
func (e *TableScanExec) Next() (Row, error) {
	if e.isVirtualTable {
		return e.nextForInfoSchema()
	}
	for {
		if e.cursor >= len(e.ranges) {
			return nil, nil
		}
		ran := e.ranges[e.cursor]
		if e.seekHandle < ran.LowVal {
			e.seekHandle = ran.LowVal
		}
		if e.seekHandle > ran.HighVal {
			e.cursor++
			continue
		}
		handle, found, err := e.t.Seek(e.ctx, e.seekHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !found {
			return nil, nil
		}
		if handle > ran.HighVal {
			// The handle is out of the current range, but may be in following ranges.
			// We seek to the range that may contains the handle, so we
			// don't need to seek key again.
			inRange := e.seekRange(handle)
			if !inRange {
				// The handle may be less than the current range low value, can not
				// return directly.
				continue
			}
		}
		row, err := e.getRow(handle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.seekHandle = handle + 1
		return row, nil
	}
}

func (e *TableScanExec) nextForInfoSchema() (Row, error) {
	if e.virtualTableRows == nil {
		columns := make([]*table.Column, e.schema.Len())
		for i, v := range e.columns {
			columns[i] = table.ToColumn(v)
		}
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			e.virtualTableRows = append(e.virtualTableRows, rec)
			return true, nil
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.virtualTableCursor >= len(e.virtualTableRows) {
		return nil, nil
	}
	row := e.virtualTableRows[e.virtualTableCursor]
	e.virtualTableCursor++
	return row, nil
}

// seekRange increments the range cursor to the range
// with high value greater or equal to handle.
func (e *TableScanExec) seekRange(handle int64) (inRange bool) {
	for {
		e.cursor++
		if e.cursor >= len(e.ranges) {
			return false
		}
		ran := e.ranges[e.cursor]
		if handle < ran.LowVal {
			return false
		}
		if handle > ran.HighVal {
			continue
		}
		return true
	}
}

func (e *TableScanExec) getRow(handle int64) (Row, error) {
	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		columns[i] = table.ToColumn(v)
	}
	row, err := e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return row, nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open() error {
	e.iter = nil
	e.cursor = 0
	return nil
}

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
