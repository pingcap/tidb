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
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	hashTable     map[string][]*Row
	smallHashKey  []*expression.Column
	bigHashKey    []*expression.Column
	smallExec     Executor
	bigExec       Executor
	prepared      bool
	ctx           context.Context
	smallFilter   expression.Expression
	bigFilter     expression.Expression
	otherFilter   expression.Expression
	schema        expression.Schema
	outer         bool
	leftSmall     bool
	cursor        int
	defaultValues []types.Datum
	// targetTypes means the target the type that both smallHashKey and bigHashKey should convert to.
	targetTypes []*types.FieldType

	finished atomic.Value
	// For sync multiple join workers.
	wg sync.WaitGroup
	// closeCh add a lock for closing executor.
	closeCh chan struct{}

	// Concurrent channels.
	concurrency      int
	bigTableRows     []chan []*Row
	bigTableErr      chan error
	hashJoinContexts []*hashJoinCtx

	// Channels for output.
	resultErr  chan error
	resultRows chan *Row
}

// hashJoinCtx holds the variables needed to do a hash join in one of many concurrent goroutines.
type hashJoinCtx struct {
	bigFilter   expression.Expression
	otherFilter expression.Expression
	// Buffer used for encode hash keys.
	datumBuffer   []types.Datum
	hashKeyBuffer []byte
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.finished.Store(true)
	<-e.closeCh
	e.prepared = false
	e.cursor = 0
	return e.smallExec.Close()
}

// makeJoinRow simply creates a new row that appends row b to row a.
func makeJoinRow(a *Row, b *Row) *Row {
	ret := &Row{
		RowKeys: make([]*RowKeyEntry, 0, len(a.RowKeys)+len(b.RowKeys)),
		Data:    make([]types.Datum, 0, len(a.Data)+len(b.Data)),
	}
	ret.RowKeys = append(ret.RowKeys, a.RowKeys...)
	ret.RowKeys = append(ret.RowKeys, b.RowKeys...)
	ret.Data = append(ret.Data, a.Data...)
	ret.Data = append(ret.Data, b.Data...)
	return ret
}

// getHashKey gets the hash key when given a row and hash columns.
// It will return a boolean value representing if the hash key has null, a byte slice representing the result hash code.
func getHashKey(sc *variable.StatementContext, cols []*expression.Column, row *Row, targetTypes []*types.FieldType,
	vals []types.Datum, bytes []byte) (bool, []byte, error) {
	var err error
	for i, col := range cols {
		vals[i], err = col.Eval(row.Data, nil)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if vals[i].IsNull() {
			return true, nil, nil
		}
		vals[i], err = vals[i].ConvertTo(sc, targetTypes[i])
		if err != nil {
			return false, nil, errors.Trace(err)
		}
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	bytes, err = codec.EncodeValue(bytes, vals...)
	return false, bytes, errors.Trace(err)
}

// Schema implements the Executor Schema interface.
func (e *HashJoinExec) Schema() expression.Schema {
	return e.schema
}

var batchSize = 128

// fetchBigExec fetches rows from the big table in a background goroutine
// and sends the rows to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchBigExec() {
	cnt := 0
	defer func() {
		for _, cn := range e.bigTableRows {
			close(cn)
		}
		e.bigExec.Close()
		e.wg.Done()
	}()
	curBatchSize := 1
	for {
		rows := make([]*Row, 0, batchSize)
		done := false
		for i := 0; i < curBatchSize; i++ {
			if e.finished.Load().(bool) {
				return
			}
			row, err := e.bigExec.Next()
			if err != nil {
				e.bigTableErr <- errors.Trace(err)
				done = true
				break
			}
			if row == nil {
				done = true
				break
			}
			rows = append(rows, row)
		}
		idx := cnt % e.concurrency
		e.bigTableRows[idx] <- rows
		cnt++
		if done {
			break
		}
		if curBatchSize < batchSize {
			curBatchSize *= 2
		}
	}
}

// prepare runs the first time when 'Next' is called, it starts one worker goroutine to fetch rows from the big table,
// and reads all data from the small table to build a hash table, then starts multiple join worker goroutines.
func (e *HashJoinExec) prepare() error {
	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.bigTableRows = make([]chan []*Row, e.concurrency)
	e.wg = sync.WaitGroup{}
	for i := 0; i < e.concurrency; i++ {
		e.bigTableRows[i] = make(chan []*Row, e.concurrency*batchSize)
	}
	e.bigTableErr = make(chan error, 1)

	// Start a worker to fetch big table rows.
	e.wg.Add(1)
	go e.fetchBigExec()

	e.hashTable = make(map[string][]*Row)
	e.cursor = 0
	sc := e.ctx.GetSessionVars().StmtCtx
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched := true
		if e.smallFilter != nil {
			matched, err = expression.EvalBool(e.smallFilter, row.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		hasNull, hashcode, err := getHashKey(sc, e.smallHashKey, row, e.targetTypes, e.hashJoinContexts[0].datumBuffer, nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []*Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}

	e.resultRows = make(chan *Row, e.concurrency*1000)
	e.resultErr = make(chan error, 1)

	for i := 0; i < e.concurrency; i++ {
		e.wg.Add(1)
		go e.runJoinWorker(i)
	}
	go e.waitJoinWorkersAndCloseResultChan()

	e.prepared = true
	return nil
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.wg.Wait()
	close(e.resultRows)
	e.hashTable = nil
	close(e.closeCh)
}

// doJoin does join job in one goroutine.
func (e *HashJoinExec) runJoinWorker(idx int) {
	for {
		var (
			bigRows []*Row
			ok      bool
			err     error
		)
		select {
		case bigRows, ok = <-e.bigTableRows[idx]:
		case err = <-e.bigTableErr:
		}
		if err != nil {
			e.resultErr <- errors.Trace(err)
			break
		}
		if !ok || e.finished.Load().(bool) {
			break
		}
		for _, bigRow := range bigRows {
			succ := e.joinOneBigRow(e.hashJoinContexts[idx], bigRow)
			if !succ {
				break
			}
		}
	}
	e.wg.Done()
}

// joinOneBigRow creates result rows from a row in a big table and sends them to resultRows channel.
// Every matching row generates a result row.
// If there are no matching rows and it is outer join, a null filled result row is created.
func (e *HashJoinExec) joinOneBigRow(ctx *hashJoinCtx, bigRow *Row) bool {
	var (
		matchedRows []*Row
		err         error
	)
	bigMatched := true
	if e.bigFilter != nil {
		bigMatched, err = expression.EvalBool(ctx.bigFilter, bigRow.Data, e.ctx)
		if err != nil {
			e.resultErr <- errors.Trace(err)
			return false
		}
	}
	if bigMatched {
		matchedRows, err = e.constructMatchedRows(ctx, bigRow)
		if err != nil {
			e.resultErr <- errors.Trace(err)
			return false
		}
	}
	for _, r := range matchedRows {
		e.resultRows <- r
	}
	if len(matchedRows) == 0 && e.outer {
		r := e.fillRowWithDefaultValues(bigRow)
		e.resultRows <- r
	}
	return true
}

// constructMatchedRows creates matching result rows from a row in the big table.
func (e *HashJoinExec) constructMatchedRows(ctx *hashJoinCtx, bigRow *Row) (matchedRows []*Row, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	hasNull, hashcode, err := getHashKey(sc, e.bigHashKey, bigRow, e.targetTypes, ctx.datumBuffer, ctx.hashKeyBuffer[0:0:cap(ctx.hashKeyBuffer)])
	if err != nil {
		return nil, errors.Trace(err)
	}

	if hasNull {
		return
	}
	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		otherMatched := true
		var matchedRow *Row
		if e.leftSmall {
			matchedRow = makeJoinRow(smallRow, bigRow)
		} else {
			matchedRow = makeJoinRow(bigRow, smallRow)
		}
		if e.otherFilter != nil {
			otherMatched, err = expression.EvalBool(ctx.otherFilter, matchedRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if otherMatched {
			matchedRows = append(matchedRows, matchedRow)
		}
	}

	return matchedRows, nil
}

// fillRowWithDefaultValues creates a result row filled with default values from a row in the big table.
// It is used for outer join, when a row from outer table doesn't have any matching rows.
func (e *HashJoinExec) fillRowWithDefaultValues(bigRow *Row) (returnRow *Row) {
	smallRow := &Row{
		Data: make([]types.Datum, e.smallExec.Schema().Len()),
	}
	copy(smallRow.Data, e.defaultValues)
	if e.leftSmall {
		returnRow = makeJoinRow(smallRow, bigRow)
	} else {
		returnRow = makeJoinRow(bigRow, smallRow)
	}
	return returnRow
}

// Next implements the Executor Next interface.
func (e *HashJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	var (
		row *Row
		err error
		ok  bool
	)
	select {
	case row, ok = <-e.resultRows:
	case err, ok = <-e.resultErr:
	}
	if err != nil {
		e.finished.Store(true)
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}
	return row, nil
}

// HashSemiJoinExec implements the hash join algorithm for semi join.
type HashSemiJoinExec struct {
	hashTable    map[string][]*Row
	smallHashKey []*expression.Column
	bigHashKey   []*expression.Column
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	ctx          context.Context
	smallFilter  expression.Expression
	bigFilter    expression.Expression
	otherFilter  expression.Expression
	schema       expression.Schema
	// In auxMode, the result row always returns with an extra column which stores a boolean
	// or NULL value to indicate if this row is matched.
	auxMode           bool
	targetTypes       []*types.FieldType
	smallTableHasNull bool
	// If anti is true, semi join only output the unmatched row.
	anti bool
}

// Close implements the Executor Close interface.
func (e *HashSemiJoinExec) Close() error {
	e.prepared = false
	e.hashTable = make(map[string][]*Row)
	e.smallTableHasNull = false
	err := e.smallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return e.bigExec.Close()
}

// Schema implements the Executor Schema interface.
func (e *HashSemiJoinExec) Schema() expression.Schema {
	return e.schema
}

// Prepare runs the first time when 'Next' is called and it reads all data from the small table and stores
// them in a hash table.
func (e *HashSemiJoinExec) prepare() error {
	e.hashTable = make(map[string][]*Row)
	sc := e.ctx.GetSessionVars().StmtCtx
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched := true
		if e.smallFilter != nil {
			matched, err = expression.EvalBool(e.smallFilter, row.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		hasNull, hashcode, err := getHashKey(sc, e.smallHashKey, row, e.targetTypes, make([]types.Datum, len(e.smallHashKey)), nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			e.smallTableHasNull = true
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []*Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}

	e.prepared = true
	return nil
}

func (e *HashSemiJoinExec) rowIsMatched(bigRow *Row) (matched bool, hasNull bool, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	hasNull, hashcode, err := getHashKey(sc, e.bigHashKey, bigRow, e.targetTypes, make([]types.Datum, len(e.smallHashKey)), nil)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	if hasNull {
		return false, true, nil
	}
	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		matched = true
		if e.otherFilter != nil {
			var matchedRow *Row
			matchedRow = makeJoinRow(bigRow, smallRow)
			matched, err = expression.EvalBool(e.otherFilter, matchedRow.Data, e.ctx)
			if err != nil {
				return false, false, errors.Trace(err)
			}
		}
		if matched {
			return
		}
	}
	return
}

// Next implements the Executor Next interface.
func (e *HashSemiJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if bigRow == nil {
			e.bigExec.Close()
			return nil, nil
		}

		matched := true
		if e.bigFilter != nil {
			matched, err = expression.EvalBool(e.bigFilter, bigRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		isNull := false
		if matched {
			matched, isNull, err = e.rowIsMatched(bigRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if !matched && e.smallTableHasNull {
			isNull = true
		}
		if e.anti && !isNull {
			matched = !matched
		}
		// For the auxMode subquery, we return the row with a Datum indicating if it's a match,
		// For the non-auxMode subquery, we return the matching row only.
		if e.auxMode {
			if isNull {
				bigRow.Data = append(bigRow.Data, types.NewDatum(nil))
			} else {
				bigRow.Data = append(bigRow.Data, types.NewDatum(matched))
			}
			return bigRow, nil
		}
		if matched {
			return bigRow, nil
		}
	}
}

// ApplyExec represents apply executor.
// Apply gets one row from outer executor and gets one row from inner executor according to outer row.
type ApplyExec struct {
	schema      expression.Schema
	Src         Executor
	outerSchema []*expression.CorrelatedColumn
	innerExec   Executor
	// checker checks if an Src row with an inner row matches the condition,
	// and if it needs to check more inner rows.
	checker *conditionChecker
}

// conditionChecker checks if all or any of the row match this condition.
type conditionChecker struct {
	cond        expression.Expression
	trimLen     int
	ctx         context.Context
	all         bool
	dataHasNull bool
}

// Check returns finished for checking if the input row can determine the final result,
// and returns data for the evaluation result.
func (c *conditionChecker) check(rowData []types.Datum) (finished bool, data types.Datum, err error) {
	data, err = c.cond.Eval(rowData, c.ctx)
	if err != nil {
		return false, data, errors.Trace(err)
	}
	var matched int64
	if data.IsNull() {
		c.dataHasNull = true
		matched = 0
	} else {
		matched, err = data.ToBool(c.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return false, data, errors.Trace(err)
		}
	}
	return (matched != 0) != c.all, data, nil
}

// Reset resets dataHasNull to false, so it can be reused for the next row from ApplyExec.Src.
func (c *conditionChecker) reset() {
	c.dataHasNull = false
}

// Schema implements the Executor Schema interface.
func (e *ApplyExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *ApplyExec) Close() error {
	if e.checker != nil {
		e.checker.dataHasNull = false
	}
	return e.Src.Close()
}

// Next implements the Executor Next interface.
func (e *ApplyExec) Next() (*Row, error) {
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	for {
		for _, col := range e.outerSchema {
			idx := col.Index
			*col.Data = srcRow.Data[idx]
		}
		innerRow, err := e.innerExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		trimLen := len(srcRow.Data)
		if innerRow != nil {
			srcRow.Data = append(srcRow.Data, innerRow.Data...)
		}
		if e.checker == nil {
			e.innerExec.Close()
			return srcRow, nil
		}
		if innerRow == nil {
			// When inner exec finishes, we need to append a result column to true, false or NULL.
			var result types.Datum
			if e.checker.all {
				result = types.NewDatum(true)
			} else {
				// If 'any' meets a null value, the result will be null.
				if e.checker.dataHasNull {
					result = types.NewDatum(nil)
				} else {
					result = types.NewDatum(false)
				}
			}
			srcRow.Data = append(srcRow.Data, result)
			e.checker.reset()
			e.innerExec.Close()
			return srcRow, nil
		}
		finished, data, err := e.checker.check(srcRow.Data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		srcRow.Data = srcRow.Data[:trimLen]
		if finished {
			e.checker.reset()
			e.innerExec.Close()
			srcRow.Data = append(srcRow.Data, data)
			return srcRow, nil
		}
	}
}
