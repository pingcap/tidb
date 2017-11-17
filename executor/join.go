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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
)

var (
	_ joinExec = &NestedLoopJoinExec{}
	_ Executor = &HashJoinExec{}

	_ joinExec = &HashSemiJoinExec{}
	_ Executor = &ApplyJoinExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerExec   Executor
	innerlExec  Executor
	outerFilter expression.CNFExprs
	innerFilter expression.CNFExprs
	outerKeys   []*expression.Column
	innerKeys   []*expression.Column

	prepared        bool
	concurrency     int // concurrency is number of concurrent channels and join workers.
	hashTable       *mvmap.MVMap
	hashJoinBuffers []*hashJoinBuffer
	outerBufferChs  []chan *execResult
	workerWaitGroup sync.WaitGroup // workerWaitGroup is for sync multiple join workers.
	finished        atomic.Value
	closeCh         chan struct{} // closeCh add a lock for closing executor.
	defaultInners   []types.Datum

	resultGenerator joinResultGenerator
	resultBufferCh  chan *execResult // Channels for output.
	resultBuffer    []Row
	resultCursor    int
}

type hashJoinBuffer struct {
	data  []types.Datum
	bytes []byte
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.finished.Store(true)
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}

	if e.prepared {
		for range e.resultBufferCh {
		}
		<-e.closeCh
	}

	e.resultBuffer = nil

	return nil
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open() error {
	if err := e.baseExecutor.Open(); err != nil {
		return errors.Trace(err)
	}

	e.prepared = false

	e.hashJoinBuffers = make([]*hashJoinBuffer, 0, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		buffer := &hashJoinBuffer{
			data:  make([]types.Datum, len(e.outerKeys)),
			bytes: make([]byte, 0, 10000),
		}
		e.hashJoinBuffers = append(e.hashJoinBuffers, buffer)
	}

	e.outerBufferChs = make([]chan *execResult, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.outerBufferChs[i] = make(chan *execResult, e.concurrency)
	}
	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.workerWaitGroup = sync.WaitGroup{}

	e.resultCursor = 0
	return nil
}

// makeJoinRow simply creates a new row that appends row b to row a.
func makeJoinRow(a Row, b Row) Row {
	ret := make([]types.Datum, 0, len(a)+len(b))
	ret = append(ret, a...)
	ret = append(ret, b...)
	return ret
}

// getJoinKey gets the hash key when given a row and hash columns.
// It will return a boolean value representing if the hash key has null, a byte slice representing the result hash code.
func getJoinKey(cols []*expression.Column, row Row, vals []types.Datum, bytes []byte) (bool, []byte, error) {
	var err error
	for i, col := range cols {
		vals[i], err = col.Eval(row)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if vals[i].IsNull() {
			return true, nil, nil
		}
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	bytes, err = codec.HashValues(bytes, vals...)
	return false, bytes, errors.Trace(err)
}

// fetchOuterRows fetches rows from the big table in a background goroutine
// and sends the rows to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterRows() {
	defer func() {
		for _, outerBufferCh := range e.outerBufferChs {
			close(outerBufferCh)
		}
		e.workerWaitGroup.Done()
	}()

	bufferCapacity, maxBufferCapacity := 1, 128
	outerBuffer := &execResult{rows: make([]Row, 0, bufferCapacity)}

	for i, noMoreData := 0, false; !noMoreData; i = (i + 1) % e.concurrency {
		for !noMoreData && len(outerBuffer.rows) < bufferCapacity {
			if e.finished.Load().(bool) {
				return
			}

			outerRow, err := e.outerExec.Next()
			if err != nil || outerRow == nil {
				outerBuffer.err = errors.Trace(err)
				noMoreData = true
				break
			}

			outerBuffer.rows = append(outerBuffer.rows, outerRow)
		}

		if noMoreData && len(outerBuffer.rows) == 0 && outerBuffer.err == nil {
			break
		}

		select {
		case <-e.ctx.GoCtx().Done():
			return
		case e.outerBufferChs[i] <- outerBuffer:
			if !noMoreData {
				if bufferCapacity < maxBufferCapacity {
					bufferCapacity <<= 1
				}
				outerBuffer = &execResult{rows: make([]Row, 0, bufferCapacity)}
			}
		}
	}
}

// prepare runs the first time when 'Next' is called, it starts one worker goroutine to fetch rows from the big table,
// and reads all data from the small table to build a hash table, then starts multiple join worker goroutines.
func (e *HashJoinExec) prepare() error {
	// Start a worker to fetch big table rows.
	e.workerWaitGroup.Add(1)
	go e.fetchOuterRows()

	e.hashTable = mvmap.NewMVMap()
	e.resultCursor = 0
	var buffer []byte
	for {
		innerRow, err := e.innerlExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if innerRow == nil {
			break
		}

		matched, err := expression.EvalBool(e.innerFilter, innerRow, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}

		hasNull, joinKey, err := getJoinKey(e.innerKeys, innerRow, e.hashJoinBuffers[0].data, nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}

		buffer = buffer[:0]
		buffer, err = e.encodeRow(buffer, innerRow)
		if err != nil {
			return errors.Trace(err)
		}

		e.hashTable.Put(joinKey, buffer)
	}

	e.resultBufferCh = make(chan *execResult, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.workerWaitGroup.Add(1)
		go e.runJoinWorker(i)
	}

	go e.waitJoinWorkersAndCloseResultChan()

	e.prepared = true
	return nil
}

func (e *HashJoinExec) encodeRow(b []byte, row Row) ([]byte, error) {
	loc := e.ctx.GetSessionVars().GetTimeZone()
	for _, datum := range row {
		tmp, err := tablecodec.EncodeValue(datum, loc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b = append(b, tmp...)
	}
	return b, nil
}

func (e *HashJoinExec) decodeRow(data []byte) (Row, error) {
	values := make([]types.Datum, e.innerlExec.Schema().Len())
	err := codec.SetRawValues(data, values)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = decodeRawValues(values, e.innerlExec.Schema(), e.ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.workerWaitGroup.Wait()
	close(e.resultBufferCh)
	close(e.closeCh)
}

// runJoinWorker does join job in one goroutine.
func (e *HashJoinExec) runJoinWorker(workerID int) {
	bufferCapacity := 1024
	resultBuffer := &execResult{rows: make([]Row, 0, bufferCapacity)}

	var outerBuffer *execResult
	for ok := true; ok; {
		select {
		case <-e.ctx.GoCtx().Done():
			ok = false
		case outerBuffer, ok = <-e.outerBufferChs[workerID]:
		}

		if !ok || e.finished.Load().(bool) {
			break
		}
		if outerBuffer.err != nil {
			resultBuffer.err = errors.Trace(outerBuffer.err)
			break
		}

		for _, outerRow := range outerBuffer.rows {
			ok = e.joinOuterRow(workerID, outerRow, resultBuffer)
			if !ok {
				break
			}
			if len(resultBuffer.rows) >= bufferCapacity {
				e.resultBufferCh <- resultBuffer
				resultBuffer = &execResult{rows: make([]Row, 0, bufferCapacity)}
			}
		}
	}

	if len(resultBuffer.rows) > 0 || resultBuffer.err != nil {
		e.resultBufferCh <- resultBuffer
	}
	e.workerWaitGroup.Done()
}

// joinOuterRow creates result rows from a row in a big table and sends them to resultRows channel.
// Every matching row generates a result row.
// If there are no matching rows and it is outer join, a null filled result row is created.
func (e *HashJoinExec) joinOuterRow(workerID int, outerRow Row, resultBuffer *execResult) bool {
	matched, err := expression.EvalBool(e.outerFilter, outerRow, e.ctx)
	if err != nil {
		resultBuffer.err = errors.Trace(err)
		return false
	}
	if !matched {
		resultBuffer.rows = e.resultGenerator.emitUnMatchedOuter(outerRow, resultBuffer.rows)
		return true
	}

	buffer := e.hashJoinBuffers[workerID]
	hasNull, joinKey, err := getJoinKey(e.outerKeys, outerRow, buffer.data, buffer.bytes[:0:cap(buffer.bytes)])
	if err != nil {
		resultBuffer.err = errors.Trace(err)
		return false
	}

	if hasNull {
		resultBuffer.rows = e.resultGenerator.emitUnMatchedOuter(outerRow, resultBuffer.rows)
		return true
	}

	values := e.hashTable.Get(joinKey)
	if len(values) == 0 {
		resultBuffer.rows = e.resultGenerator.emitUnMatchedOuter(outerRow, resultBuffer.rows)
		return true
	}

	innerRows := make([]Row, 0, len(values))
	for _, value := range values {
		innerRow, err := e.decodeRow(value)
		if err != nil {
			resultBuffer.err = errors.Trace(err)
			return false
		}
		innerRows = append(innerRows, innerRow)
	}

	resultBuffer.rows, matched, err = e.resultGenerator.emitMatchedInners(outerRow, innerRows, resultBuffer.rows)
	if err != nil {
		resultBuffer.err = errors.Trace(err)
		return false
	}
	if !matched {
		resultBuffer.rows = e.resultGenerator.emitUnMatchedOuter(outerRow, resultBuffer.rows)
	}
	return true
}

// Next implements the Executor Next interface.
func (e *HashJoinExec) Next() (Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if e.resultCursor >= len(e.resultBuffer) {
		e.resultCursor = 0
		select {
		case resultBuffer, ok := <-e.resultBufferCh:
			if !ok {
				return nil, nil
			}
			if resultBuffer.err != nil {
				e.finished.Store(true)
				return nil, errors.Trace(resultBuffer.err)
			}
			e.resultBuffer = resultBuffer.rows
		case <-e.ctx.GoCtx().Done():
			return nil, nil
		}
	}

	// len(e.resultBuffer) > 0 is guaranteed in the above "select".
	result := e.resultBuffer[e.resultCursor]
	e.resultCursor++
	return result, nil
}

// joinExec is the common interface of join algorithm except for hash join.
type joinExec interface {
	Executor

	// fetchBigRow fetches a valid row from big Exec and returns a bool value that means if it is matched.
	fetchBigRow() (Row, bool, error)
	// prepare reads all records from small Exec and stores them.
	prepare() error
	// doJoin fetches a row from big exec and a bool value that means if it's matched with big filter,
	// then get all the rows matches the on condition.
	doJoin(Row, bool) ([]Row, error)
}

// NestedLoopJoinExec implements nested-loop algorithm for join.
type NestedLoopJoinExec struct {
	baseExecutor

	innerRows     []Row
	cursor        int
	resultRows    []Row
	SmallExec     Executor
	BigExec       Executor
	leftSmall     bool
	prepared      bool
	SmallFilter   expression.CNFExprs
	BigFilter     expression.CNFExprs
	OtherFilter   expression.CNFExprs
	outer         bool
	defaultValues []types.Datum
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

		matched, err := expression.EvalBool(e.BigFilter, bigRow, e.ctx)
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

		matched, err := expression.EvalBool(e.SmallFilter, row, e.ctx)
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
		matched, err := expression.EvalBool(e.OtherFilter, mergedRow, e.ctx)
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

// HashSemiJoinExec implements the hash join algorithm for semi join.
type HashSemiJoinExec struct {
	baseExecutor

	hashTable    map[string][]Row
	smallHashKey []*expression.Column
	bigHashKey   []*expression.Column
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	smallFilter  expression.CNFExprs
	bigFilter    expression.CNFExprs
	otherFilter  expression.CNFExprs
	resultRows   []Row
	// auxMode is a mode that the result row always returns with an extra column which stores a boolean
	// or NULL value to indicate if this row is matched.
	auxMode           bool
	smallTableHasNull bool
	// anti is true, semi join only output the unmatched row.
	anti bool
}

// Close implements the Executor Close interface.
func (e *HashSemiJoinExec) Close() error {
	e.hashTable = nil
	e.resultRows = nil
	return e.bigExec.Close()
}

// Open implements the Executor Open interface.
func (e *HashSemiJoinExec) Open() error {
	e.prepared = false
	e.smallTableHasNull = false
	e.hashTable = make(map[string][]Row)
	e.resultRows = make([]Row, 1)
	return errors.Trace(e.bigExec.Open())
}

// prepare runs the first time when 'Next' is called and it reads all data from the small table and stores
// them in a hash table.
func (e *HashSemiJoinExec) prepare() error {
	err := e.smallExec.Open()
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(e.smallExec.Close)
	e.hashTable = make(map[string][]Row)
	e.resultRows = make([]Row, 1)
	e.prepared = true
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}

		matched, err := expression.EvalBool(e.smallFilter, row, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}
		hasNull, hashcode, err := getJoinKey(e.smallHashKey, row, make([]types.Datum, len(e.smallHashKey)), nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			e.smallTableHasNull = true
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}
}

func (e *HashSemiJoinExec) rowIsMatched(bigRow Row) (matched bool, hasNull bool, err error) {
	hasNull, hashcode, err := getJoinKey(e.bigHashKey, bigRow, make([]types.Datum, len(e.smallHashKey)), nil)
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
		matched, err = expression.EvalBool(e.otherFilter, matchedRow, e.ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		if matched {
			return
		}
	}
	return
}

func (e *HashSemiJoinExec) fetchBigRow() (Row, bool, error) {
	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if bigRow == nil {
			return nil, false, errors.Trace(e.bigExec.Close())
		}

		matched, err := expression.EvalBool(e.bigFilter, bigRow, e.ctx)
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

func (e *HashSemiJoinExec) doJoin(bigRow Row, match bool) ([]Row, error) {
	if e.auxMode && !match {
		bigRow = append(bigRow, types.NewDatum(false))
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
			bigRow = append(bigRow, types.NewDatum(nil))
		} else {
			bigRow = append(bigRow, types.NewDatum(matched))
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
func (e *HashSemiJoinExec) Next() (Row, error) {
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
	baseExecutor

	join        joinExec
	outerSchema []*expression.CorrelatedColumn
	cursor      int
	resultRows  []Row
}

// Close implements the Executor interface.
func (e *ApplyJoinExec) Close() error {
	return e.join.Close()
}

// Open implements the Executor interface.
func (e *ApplyJoinExec) Open() error {
	e.cursor = 0
	e.resultRows = nil
	return errors.Trace(e.join.Open())
}

// Next implements the Executor interface.
func (e *ApplyJoinExec) Next() (Row, error) {
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
			*col.Data = bigRow[col.Index]
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
