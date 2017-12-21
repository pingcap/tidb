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
	"bytes"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	goctx "golang.org/x/net/context"
)

type keyRowBlock struct {
	keys        [][]byte
	rows        []Row
	requestRows [][]types.Datum
}

func newKeyRowBlock(batchSize int, hasRequestRow bool) *keyRowBlock {
	result := &keyRowBlock{
		keys: make([][]byte, 0, batchSize),
		rows: make([]Row, 0, batchSize),
	}
	if hasRequestRow {
		result.requestRows = make([][]types.Datum, 0, batchSize)
	}
	return result
}

// Len returns the number of rows.
func (e *keyRowBlock) Len() int {
	return len(e.rows)
}

// Swap implements sort.Interface Swap interface.
func (e *keyRowBlock) Swap(i, j int) {
	e.keys[i], e.keys[j] = e.keys[j], e.keys[i]
	e.rows[i], e.rows[j] = e.rows[j], e.rows[i]
	if e.requestRows != nil {
		e.requestRows[i], e.requestRows[j] = e.requestRows[j], e.requestRows[i]
	}
}

// Less implements sort.Interface Less interface.
func (e *keyRowBlock) Less(i, j int) bool {
	return e.compare(e, i, j) < 0
}

func (e *keyRowBlock) compare(other *keyRowBlock, i, j int) int {
	return bytes.Compare(e.keys[i], other.keys[j])
}

func (e *keyRowBlock) nextBatch(i int) int {
	for i++; i < len(e.rows); i++ {
		if e.Less(i-1, i) {
			return i
		}
	}
	return len(e.rows)
}

func (e *keyRowBlock) reset() {
	e.keys = e.keys[:0:cap(e.keys)]
	e.rows = e.rows[:0:cap(e.rows)]
	if e.requestRows != nil {
		e.requestRows = e.requestRows[:0:cap(e.requestRows)]
	}
}

// IndexLookUpJoin fetches batches of data from outer executor and constructs ranges for inner executor.
type IndexLookUpJoin struct {
	baseExecutor

	outerExec        Executor
	innerExecBuilder *dataReaderBuilder
	outerKeys        []*expression.Column
	innerKeys        []*expression.Column
	outerFilter      expression.CNFExprs
	innerFilter      expression.CNFExprs
	outerOrderedRows *keyRowBlock
	innerOrderedRows *keyRowBlock

	resultGenerator joinResultGenerator
	resultBuffer    []Row
	resultCursor    int

	buffer4JoinKeys [][]types.Datum
	buffer4JoinKey  []types.Datum

	maxBatchSize int
	curBatchSize int
	exhausted    bool // exhausted means whether all data has been extracted.

	indexRanges   []*ranger.IndexRange
	keyOff2IdxOff []int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpJoin) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}

	e.outerOrderedRows = newKeyRowBlock(e.maxBatchSize, true)
	e.innerOrderedRows = newKeyRowBlock(e.maxBatchSize, false)
	e.resultCursor = 0
	e.curBatchSize = 32
	e.resultBuffer = make([]Row, 0, e.maxBatchSize)
	e.buffer4JoinKeys = make([][]types.Datum, 0, e.maxBatchSize)
	e.buffer4JoinKey = make([]types.Datum, 0, e.maxBatchSize*len(e.outerKeys))
	e.exhausted = false
	return nil
}

// Close implements the Executor Close interface.
func (e *IndexLookUpJoin) Close() error {
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}

	// release all resource references.
	e.outerOrderedRows = nil
	e.innerOrderedRows = nil
	e.resultBuffer = nil
	e.buffer4JoinKeys = nil
	e.buffer4JoinKey = nil

	return nil
}

// Next implements the Executor Next interface.
// Step1: fetch a batch of "outer rows".
// Step2: construct a batch of "request rows" based on the outer rows.
// Step3: construct "join keys" based on the request rows.
// Step4: sort "outer rows", "request rows" based on "join keys".
// Step5: deduplicate "request rows" based on the join keys.
// Step6: fetch a batch of sorted "inner rows" based on the request rows.
// Step7: do merge join on the **sorted** outer and inner rows.
func (e *IndexLookUpJoin) Next(goCtx goctx.Context) (Row, error) {
	for ; e.resultCursor == len(e.resultBuffer); e.resultCursor = 0 {
		if e.curBatchSize < e.maxBatchSize {
			e.curBatchSize *= 2
		}
		if e.exhausted {
			return nil, nil
		}

		e.outerOrderedRows.reset()
		e.innerOrderedRows.reset()
		e.resultBuffer = e.resultBuffer[:0:cap(e.resultBuffer)]

		for i := 0; i < e.curBatchSize; i++ {
			outerRow, err := e.outerExec.Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			} else if outerRow == nil {
				e.exhausted = true
				break
			}

			matched, err := expression.EvalBool(e.outerFilter, outerRow, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			} else if !matched {
				e.resultBuffer, err = e.resultGenerator.emit(outerRow, nil, e.resultBuffer)
				if err != nil {
					return nil, errors.Trace(err)
				}
			} else {
				e.outerOrderedRows.rows = append(e.outerOrderedRows.rows, outerRow)
			}
		}

		if len(e.outerOrderedRows.rows) == 0 {
			continue
		}

		var err error
		e.outerOrderedRows.requestRows, err = e.constructRequestRows(e.outerOrderedRows.rows)
		if err != nil {
			return nil, errors.Trace(err)
		}

		e.outerOrderedRows.keys, err = e.constructJoinKeys(e.outerOrderedRows.requestRows)
		if err != nil {
			return nil, errors.Trace(err)
		}

		sort.Sort(e.outerOrderedRows)
		requestRows := e.deDuplicateRequestRows(e.outerOrderedRows.requestRows, e.outerOrderedRows.keys)

		if err = e.fetchSortedInners(requestRows); err != nil {
			return nil, errors.Trace(err)
		}
		if err = e.doMergeJoin(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	result := e.resultBuffer[e.resultCursor]
	e.resultCursor++
	return result, nil
}

func (e *IndexLookUpJoin) constructRequestRows(outerRows []Row) ([][]types.Datum, error) {
	requestRows := e.buffer4JoinKeys[:0]
	requestRow := e.buffer4JoinKey[:0]
	for _, outerRow := range outerRows {
		for i, outerKey := range e.outerKeys {
			outerDatum, err := outerKey.Eval(outerRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
			innerDatum, err := outerDatum.ConvertTo(e.ctx.GetSessionVars().StmtCtx, e.innerKeys[i].GetType())
			if err != nil {
				return nil, errors.Trace(err)
			}
			requestRow = append(requestRow, innerDatum)
		}
		requestRows = append(requestRows, requestRow)
		requestRow = requestRow[len(requestRow):]
	}
	return requestRows, nil
}

func (e *IndexLookUpJoin) constructJoinKeys(joinKeys [][]types.Datum) ([][]byte, error) {
	keys := make([][]byte, 0, len(joinKeys))
	for _, joinKey := range joinKeys {
		key, err := codec.EncodeKey(nil, joinKey...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// deDuplicateRequestRows removes the duplicated rows in requestRows.
// NOTE: Every request row have a corresponding request key, "requestRows" must be ordered by "requestKeys".
// NOTE: The caller should guarantee that "len(requestRows) > 0".
func (e *IndexLookUpJoin) deDuplicateRequestRows(requestRows [][]types.Datum, requestKeys [][]byte) [][]types.Datum {
	noDuplicateRequestRows := requestRows[:1:len(requestRows)]
	for i, length := 1, len(requestRows); i < length; i++ {
		if !bytes.Equal(requestKeys[i], requestKeys[i-1]) {
			noDuplicateRequestRows = append(noDuplicateRequestRows, requestRows[i])
		}
	}
	return noDuplicateRequestRows
}

// fetchSortedInners will join the outer rows and inner rows and store them to resultBuffer.
func (e *IndexLookUpJoin) fetchSortedInners(requestRows [][]types.Datum) error {
	goCtx := goctx.TODO()
	innerExec, err := e.innerExecBuilder.buildExecutorForIndexJoin(goCtx, requestRows, e.indexRanges, e.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)

	for {
		innerRow, err1 := innerExec.Next(goCtx)
		if err1 != nil {
			return errors.Trace(err1)
		} else if innerRow == nil {
			break
		}

		matched, err2 := expression.EvalBool(e.innerFilter, innerRow, e.ctx)
		if err2 != nil {
			return errors.Trace(err2)
		} else if matched {
			e.innerOrderedRows.rows = append(e.innerOrderedRows.rows, innerRow)
		}
	}

	innerJoinKeys := e.buffer4JoinKeys[:0]
	innerJoinKey := e.buffer4JoinKey[:0]
	for _, innerRow := range e.innerOrderedRows.rows {
		for _, innerKey := range e.innerKeys {
			innerDatum, err1 := innerKey.Eval(innerRow)
			if err1 != nil {
				return errors.Trace(err1)
			}
			innerJoinKey = append(innerJoinKey, innerDatum)
		}
		innerJoinKeys = append(innerJoinKeys, innerJoinKey)
		innerJoinKey = innerJoinKey[len(innerJoinKey):]
	}

	e.innerOrderedRows.keys, err = e.constructJoinKeys(innerJoinKeys)
	if err != nil {
		return errors.Trace(err)
	}

	sort.Sort(e.innerOrderedRows)
	return nil
}

// doMergeJoin joins the innerOrderedRows and outerOrderedRows which have been sorted before.
func (e *IndexLookUpJoin) doMergeJoin() (err error) {
	outerCursor, innerCursor := 0, 0
	for outerCursor < len(e.outerOrderedRows.rows) && innerCursor < len(e.innerOrderedRows.rows) {
		compareResult := e.outerOrderedRows.compare(e.innerOrderedRows, outerCursor, innerCursor)
		switch {
		case compareResult > 0:
			innerCursor = e.innerOrderedRows.nextBatch(innerCursor)
		case compareResult < 0:
			outerNextCursor := e.outerOrderedRows.nextBatch(outerCursor)
			for _, unMatchedOuter := range e.outerOrderedRows.rows[outerCursor:outerNextCursor] {
				e.resultBuffer, err = e.resultGenerator.emit(unMatchedOuter, nil, e.resultBuffer)
				if err != nil {
					return errors.Trace(err)
				}
			}
			outerCursor = outerNextCursor
		case compareResult == 0:
			outerNextCursor := e.outerOrderedRows.nextBatch(outerCursor)
			innerNextCursor := e.innerOrderedRows.nextBatch(innerCursor)
			for _, outerRow := range e.outerOrderedRows.rows[outerCursor:outerNextCursor] {
				e.resultBuffer, err = e.resultGenerator.emit(outerRow, e.innerOrderedRows.rows[innerCursor:innerNextCursor], e.resultBuffer)
				if err != nil {
					return errors.Trace(err)
				}
			}
			outerCursor, innerCursor = outerNextCursor, innerNextCursor
		}
	}
	for _, unMatchedOuter := range e.outerOrderedRows.rows[outerCursor:] {
		e.resultBuffer, err = e.resultGenerator.emit(unMatchedOuter, nil, e.resultBuffer)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
