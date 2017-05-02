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
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ joinExec = &NestedLoopJoinExec{}
	_ Executor = &HashJoinExec{}
	_ joinExec = &HashSemiJoinExec{}
	_ Executor = &ApplyJoinExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	hashTable     *mvmap.MVMap
	smallHashKey  []*expression.Column
	bigHashKey    []*expression.Column
	smallExec     Executor
	bigExec       Executor
	prepared      bool
	ctx           context.Context
	smallFilter   expression.CNFExprs
	bigFilter     expression.CNFExprs
	otherFilter   expression.CNFExprs
	schema        *expression.Schema
	outer         bool
	leftSmall     bool
	cursor        int
	defaultValues []types.Datum
	// targetTypes means the target the type that both smallHashKey and bigHashKey should convert to.
	targetTypes []*types.FieldType

	finished atomic.Value
	// wg is for sync multiple join workers.
	wg sync.WaitGroup
	// closeCh add a lock for closing executor.
	closeCh chan struct{}

	rows []*Row
	// concurrency is number of concurrent channels.
	concurrency      int
	bigTableResultCh []chan *execResult
	hashJoinContexts []*hashJoinCtx

	// Channels for output.
	resultCh chan *execResult

	// rowKeyCache is used to store the table and table name from a row.
	// Because every row has the same table name and table, we can use a single row key cache.
	rowKeyCache []*RowKeyEntry
}

// hashJoinCtx holds the variables needed to do a hash join in one of many concurrent goroutines.
type hashJoinCtx struct {
	bigFilter   expression.CNFExprs
	otherFilter expression.CNFExprs
	// datumBuffer is used for encode hash keys.
	datumBuffer   []types.Datum
	hashKeyBuffer []byte
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.finished.Store(true)
	if e.prepared {
		for range e.resultCh {
		}
		<-e.closeCh
	}
	e.prepared = false
	e.cursor = 0
	e.rows = nil
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

// getJoinKey gets the hash key when given a row and hash columns.
// It will return a boolean value representing if the hash key has null, a byte slice representing the result hash code.
func getJoinKey(sc *variable.StatementContext, cols []*expression.Column, row *Row, targetTypes []*types.FieldType,
	vals []types.Datum, bytes []byte) (bool, []byte, error) {
	var err error
	for i, col := range cols {
		vals[i], err = col.Eval(row.Data)
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
func (e *HashJoinExec) Schema() *expression.Schema {
	return e.schema
}

var batchSize = 128

// fetchBigExec fetches rows from the big table in a background goroutine
// and sends the rows to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchBigExec() {
	cnt := 0
	defer func() {
		for _, cn := range e.bigTableResultCh {
			close(cn)
		}
		e.bigExec.Close()
		e.wg.Done()
	}()
	curBatchSize := 1
	result := &execResult{rows: make([]*Row, 0, curBatchSize)}
	txnCtx := e.ctx.GoCtx()
	for {
		done := false
		idx := cnt % e.concurrency
		for i := 0; i < curBatchSize; i++ {
			if e.finished.Load().(bool) {
				return
			}
			row, err := e.bigExec.Next()
			if err != nil {
				result.err = errors.Trace(err)
				e.bigTableResultCh[idx] <- result
				done = true
				break
			}
			if row == nil {
				done = true
				break
			}
			result.rows = append(result.rows, row)
			if len(result.rows) >= curBatchSize {
				select {
				case <-txnCtx.Done():
					return
				case e.bigTableResultCh[idx] <- result:
					result = &execResult{rows: make([]*Row, 0, curBatchSize)}
				}
			}
		}
		cnt++
		if done {
			if len(result.rows) > 0 {
				select {
				case <-txnCtx.Done():
					return
				case e.bigTableResultCh[idx] <- result:
				}
			}
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
	e.bigTableResultCh = make([]chan *execResult, e.concurrency)
	e.wg = sync.WaitGroup{}
	for i := 0; i < e.concurrency; i++ {
		e.bigTableResultCh[i] = make(chan *execResult, e.concurrency)
	}
	// Start a worker to fetch big table rows.
	e.wg.Add(1)
	go e.fetchBigExec()

	e.hashTable = mvmap.NewMVMap()
	e.cursor = 0
	sc := e.ctx.GetSessionVars().StmtCtx
	var buffer []byte
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched, err := expression.EvalBool(e.smallFilter, row.Data, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}
		hasNull, joinKey, err := getJoinKey(sc, e.smallHashKey, row, e.targetTypes, e.hashJoinContexts[0].datumBuffer, nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}
		buffer = buffer[:0]
		buffer, err = e.encodeRow(buffer, row)
		if err != nil {
			return errors.Trace(err)
		}
		e.hashTable.Put(joinKey, buffer)
	}

	e.resultCh = make(chan *execResult, e.concurrency)

	for i := 0; i < e.concurrency; i++ {
		e.wg.Add(1)
		go e.runJoinWorker(i)
	}
	go e.waitJoinWorkersAndCloseResultChan()

	e.prepared = true
	return nil
}

func (e *HashJoinExec) encodeRow(b []byte, row *Row) ([]byte, error) {
	numRowKeys := int64(len(row.RowKeys))
	b = codec.EncodeVarint(b, numRowKeys)
	for _, rowKey := range row.RowKeys {
		b = codec.EncodeVarint(b, rowKey.Handle)
	}
	if numRowKeys > 0 && e.rowKeyCache == nil {
		e.rowKeyCache = make([]*RowKeyEntry, len(row.RowKeys))
		for i := 0; i < len(row.RowKeys); i++ {
			rk := new(RowKeyEntry)
			rk.Tbl = row.RowKeys[i].Tbl
			rk.TableName = row.RowKeys[i].TableName
			e.rowKeyCache[i] = rk
		}
	}
	b, err := codec.EncodeValue(b, row.Data...)
	return b, errors.Trace(err)
}

func (e *HashJoinExec) decodeRow(data []byte) (*Row, error) {
	row := new(Row)
	data, entryLen, err := codec.DecodeVarint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := 0; i < int(entryLen); i++ {
		entry := new(RowKeyEntry)
		data, entry.Handle, err = codec.DecodeVarint(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		entry.Tbl = e.rowKeyCache[i].Tbl
		entry.TableName = e.rowKeyCache[i].TableName
		row.RowKeys = append(row.RowKeys, entry)
	}
	values := make([]types.Datum, e.smallExec.Schema().Len())
	err = codec.SetRawValues(data, values)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = decodeRawValues(values, e.smallExec.Schema())
	if err != nil {
		return nil, errors.Trace(err)
	}
	row.Data = values
	return row, nil
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.wg.Wait()
	close(e.resultCh)
	e.hashTable = nil
	close(e.closeCh)
}

// runJoinWorker does join job in one goroutine.
func (e *HashJoinExec) runJoinWorker(idx int) {
	maxRowsCnt := 1000
	result := &execResult{rows: make([]*Row, 0, maxRowsCnt)}
	txnCtx := e.ctx.GoCtx()
	for {
		var bigTableResult *execResult
		var exit bool
		select {
		case <-txnCtx.Done():
			exit = true
		case tmp, ok := <-e.bigTableResultCh[idx]:
			if !ok {
				exit = true
			}
			bigTableResult = tmp
		}
		if exit || e.finished.Load().(bool) {
			break
		}

		if bigTableResult.err != nil {
			e.resultCh <- &execResult{err: errors.Trace(bigTableResult.err)}
			break
		}
		for _, bigRow := range bigTableResult.rows {
			succ := e.joinOneBigRow(e.hashJoinContexts[idx], bigRow, result)
			if !succ {
				break
			}
			if len(result.rows) >= maxRowsCnt {
				e.resultCh <- result
				result = &execResult{rows: make([]*Row, 0, maxRowsCnt)}
			}
		}
	}
	if len(result.rows) != 0 || result.err != nil {
		e.resultCh <- result
	}
	e.wg.Done()
}

// joinOneBigRow creates result rows from a row in a big table and sends them to resultRows channel.
// Every matching row generates a result row.
// If there are no matching rows and it is outer join, a null filled result row is created.
func (e *HashJoinExec) joinOneBigRow(ctx *hashJoinCtx, bigRow *Row, result *execResult) bool {
	var (
		matchedRows []*Row
		err         error
	)
	bigMatched := true
	bigMatched, err = expression.EvalBool(ctx.bigFilter, bigRow.Data, e.ctx)
	if err != nil {
		result.err = errors.Trace(err)
		return false
	}
	if bigMatched {
		matchedRows, err = e.constructMatchedRows(ctx, bigRow)
		if err != nil {
			result.err = errors.Trace(err)
			return false
		}
	}
	for _, r := range matchedRows {
		result.rows = append(result.rows, r)
	}
	if len(matchedRows) == 0 && e.outer {
		r := e.fillRowWithDefaultValues(bigRow)
		result.rows = append(result.rows, r)
	}
	return true
}

// constructMatchedRows creates matching result rows from a row in the big table.
func (e *HashJoinExec) constructMatchedRows(ctx *hashJoinCtx, bigRow *Row) (matchedRows []*Row, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	hasNull, joinKey, err := getJoinKey(sc, e.bigHashKey, bigRow, e.targetTypes, ctx.datumBuffer, ctx.hashKeyBuffer[0:0:cap(ctx.hashKeyBuffer)])
	if err != nil {
		return nil, errors.Trace(err)
	}

	if hasNull {
		return
	}
	values := e.hashTable.Get(joinKey)
	if len(values) == 0 {
		return
	}
	// match eq condition
	for _, value := range values {
		var smallRow *Row
		smallRow, err = e.decodeRow(value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var matchedRow *Row
		if e.leftSmall {
			matchedRow = makeJoinRow(smallRow, bigRow)
		} else {
			matchedRow = makeJoinRow(bigRow, smallRow)
		}
		otherMatched, err := expression.EvalBool(ctx.otherFilter, matchedRow.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
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
	txnCtx := e.ctx.GoCtx()
	if e.cursor >= len(e.rows) {
		var result *execResult
		select {
		case tmp, ok := <-e.resultCh:
			if !ok {
				return nil, nil
			}
			result = tmp
			if result.err != nil {
				e.finished.Store(true)
				return nil, errors.Trace(result.err)
			}
		case <-txnCtx.Done():
			return nil, nil
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

// joinExec is the common interface of join algorithm except for hash join.
type joinExec interface {
	Executor

	// fetchBigRow fetches a valid row from big Exec and returns a bool value that means if it is matched.
	fetchBigRow() (*Row, bool, error)
	// prepare reads all records from small Exec and stores them.
	prepare() error
	// doJoin fetches a row from big exec and a bool value that means if it's matched with big filter,
	// then get all the rows matches the on condition.
	doJoin(*Row, bool) ([]*Row, error)
}

// NestedLoopJoinExec implements nested-loop algorithm for join.
type NestedLoopJoinExec struct {
	innerRows     []*Row
	cursor        int
	resultRows    []*Row
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
	e.cursor = 0
	e.prepared = false
	err := e.BigExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return e.SmallExec.Close()
}

func (e *NestedLoopJoinExec) fetchBigRow() (*Row, bool, error) {
	for {
		bigRow, err := e.BigExec.Next()
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if bigRow == nil {
			return nil, false, e.BigExec.Close()
		}

		matched, err := expression.EvalBool(e.BigFilter, bigRow.Data, e.Ctx)
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
	err := e.SmallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	e.innerRows = e.innerRows[:0]
	e.prepared = true
	for {
		row, err := e.SmallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return e.SmallExec.Close()
		}

		matched, err := expression.EvalBool(e.SmallFilter, row.Data, e.Ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			e.innerRows = append(e.innerRows, row)
		}
	}
}

func (e *NestedLoopJoinExec) fillRowWithDefaultValue(bigRow *Row) (returnRow *Row) {
	smallRow := &Row{
		Data: make([]types.Datum, e.SmallExec.Schema().Len()),
	}
	copy(smallRow.Data, e.defaultValues)
	if e.leftSmall {
		returnRow = makeJoinRow(smallRow, bigRow)
	} else {
		returnRow = makeJoinRow(bigRow, smallRow)
	}
	return returnRow
}

func (e *NestedLoopJoinExec) doJoin(bigRow *Row, match bool) ([]*Row, error) {
	e.resultRows = e.resultRows[0:0]
	if !match && e.outer {
		row := e.fillRowWithDefaultValue(bigRow)
		e.resultRows = append(e.resultRows, row)
		return e.resultRows, nil
	}
	for _, row := range e.innerRows {
		var mergedRow *Row
		if e.leftSmall {
			mergedRow = makeJoinRow(row, bigRow)
		} else {
			mergedRow = makeJoinRow(bigRow, row)
		}
		matched, err := expression.EvalBool(e.OtherFilter, mergedRow.Data, e.Ctx)
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
func (e *NestedLoopJoinExec) Next() (*Row, error) {
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

// HashSemiJoinExec implements the hash join algorithm for semi join.
type HashSemiJoinExec struct {
	hashTable    map[string][]*Row
	smallHashKey []*expression.Column
	bigHashKey   []*expression.Column
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	ctx          context.Context
	smallFilter  expression.CNFExprs
	bigFilter    expression.CNFExprs
	otherFilter  expression.CNFExprs
	schema       *expression.Schema
	resultRows   []*Row
	// auxMode is a mode that the result row always returns with an extra column which stores a boolean
	// or NULL value to indicate if this row is matched.
	auxMode           bool
	targetTypes       []*types.FieldType
	smallTableHasNull bool
	// anti is true, semi join only output the unmatched row.
	anti bool
}

// Close implements the Executor Close interface.
func (e *HashSemiJoinExec) Close() error {
	e.prepared = false
	e.hashTable = make(map[string][]*Row)
	e.smallTableHasNull = false
	e.resultRows = nil
	err := e.smallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return e.bigExec.Close()
}

// Schema implements the Executor Schema interface.
func (e *HashSemiJoinExec) Schema() *expression.Schema {
	return e.schema
}

// prepare runs the first time when 'Next' is called and it reads all data from the small table and stores
// them in a hash table.
func (e *HashSemiJoinExec) prepare() error {
	err := e.smallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	e.hashTable = make(map[string][]*Row)
	sc := e.ctx.GetSessionVars().StmtCtx
	e.resultRows = make([]*Row, 1)
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched, err := expression.EvalBool(e.smallFilter, row.Data, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}
		hasNull, hashcode, err := getJoinKey(sc, e.smallHashKey, row, e.targetTypes, make([]types.Datum, len(e.smallHashKey)), nil)
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
	hasNull, hashcode, err := getJoinKey(sc, e.bigHashKey, bigRow, e.targetTypes, make([]types.Datum, len(e.smallHashKey)), nil)
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
		matchedRow := makeJoinRow(bigRow, smallRow)
		matched, err = expression.EvalBool(e.otherFilter, matchedRow.Data, e.ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		if matched {
			return
		}
	}
	return
}

func (e *HashSemiJoinExec) fetchBigRow() (*Row, bool, error) {
	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if bigRow == nil {
			return nil, false, errors.Trace(e.bigExec.Close())
		}

		matched, err := expression.EvalBool(e.bigFilter, bigRow.Data, e.ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			return bigRow, true, nil
		} else if e.auxMode {
			return bigRow, false, nil
		}
	}
}

func (e *HashSemiJoinExec) doJoin(bigRow *Row, match bool) ([]*Row, error) {
	if e.auxMode && !match {
		bigRow.Data = append(bigRow.Data, types.NewDatum(false))
		e.resultRows[0] = bigRow
		return e.resultRows, nil
	}
	matched, isNull, err := e.rowIsMatched(bigRow)
	if err != nil {
		return nil, errors.Trace(err)
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
		matched = true
	}
	if matched {
		e.resultRows[0] = bigRow
		return e.resultRows, nil
	}
	return nil, nil
}

// Next implements the Executor Next interface.
func (e *HashSemiJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for {
		bigRow, match, err := e.fetchBigRow()
		if bigRow == nil || err != nil {
			return bigRow, errors.Trace(err)
		}
		resultRows, err := e.doJoin(bigRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(resultRows) > 0 {
			return resultRows[0], nil
		}
	}
}

// ApplyJoinExec is the new logic of apply.
type ApplyJoinExec struct {
	join        joinExec
	outerSchema []*expression.CorrelatedColumn
	cursor      int
	resultRows  []*Row
	schema      *expression.Schema
}

// Schema implements the Executor interface.
func (e *ApplyJoinExec) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor interface.
func (e *ApplyJoinExec) Close() error {
	e.cursor = 0
	e.resultRows = nil
	return e.join.Close()
}

// Next implements the Executor interface.
func (e *ApplyJoinExec) Next() (*Row, error) {
	for {
		if e.cursor < len(e.resultRows) {
			row := e.resultRows[e.cursor]
			e.cursor++
			return row, nil
		}
		bigRow, match, err := e.join.fetchBigRow()
		if bigRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		for _, col := range e.outerSchema {
			*col.Data = bigRow.Data[col.Index]
		}
		err = e.join.prepare()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.resultRows, err = e.join.doJoin(bigRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.cursor = 0
	}
}
