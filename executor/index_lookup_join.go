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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

type keyRowBlock struct {
	keys        [][]byte
	rows        []Row
	requestRows [][]types.Datum
}

// Len returns the number of rows.
func (e keyRowBlock) Len() int {
	return len(e.rows)
}

// Swap implements sort.Interface Swap interface.
func (e keyRowBlock) Swap(i, j int) {
	e.keys[i], e.keys[j] = e.keys[j], e.keys[j]
	e.rows[i], e.rows[j] = e.rows[j], e.rows[i]
	if e.requestRows != nil {
		e.requestRows[i], e.requestRows[j] = e.requestRows[j], e.requestRows[i]
	}
}

// Less implements sort.Interface Less interface.
func (e keyRowBlock) Less(i, j int) bool {
	return bytes.Compare(e.keys[i], e.keys[j]) < 0
}

// IndexLookUpJoin fetches batches of data from outer executor and constructs ranges for inner executor.
type IndexLookUpJoin struct {
	baseExecutor

	outerExec        Executor
	innerExec        DataReader
	outerKeys        []*expression.Column
	innerKeys        []*expression.Column
	outerFilter      expression.CNFExprs
	innerFilter      expression.CNFExprs
	innerRows        []Row
	outerOrderedRows keyRowBlock
	innerOrderedRows keyRowBlock

	innerDatums      keyRowBlock // innerDatums are extracted by innerOrderedRows and innerKeys
	innerRequestRows []Row

	outer     bool
	exhausted bool // exhausted means whether all data has been extracted

	resultFilter    expression.CNFExprs
	resultGenerator joinResultGenerator
	resultBuffer    []Row
	resultCursor    int
	batchSize       int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpJoin) Open() error {
	e.resultCursor = 0
	e.exhausted = false
	return errors.Trace(e.children[0].Open())
}

// Close implements the Executor Close interface.
func (e *IndexLookUpJoin) Close() error {
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Next implements the Executor Next interface.
// We will fetch batches of row from outer executor, construct the inner datums and sort them.
// At the same time we will fetch the inner row by the inner datums and apply merge join.
func (e *IndexLookUpJoin) Next() (result Row, err error) {
	for ; e.resultCursor == len(e.resultBuffer); e.resultCursor = 0 {
		if e.exhausted {
			return nil, nil
		}

		e.resultBuffer = e.resultBuffer[:0]
		e.outerOrderedRows.rows = e.outerOrderedRows.rows[:0]

		for i := 0; i < e.batchSize; i++ {
			outerRow, err := e.outerExec.Next()
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
				e.resultBuffer = e.resultGenerator.emitUnMatchedOuter(outerRow, e.resultBuffer)
				continue
			}

			e.outerOrderedRows.rows = append(e.outerOrderedRows.rows, outerRow)
		}

		e.outerOrderedRows.requestRows, err = e.constructRequestRows(e.outerOrderedRows.rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.outerOrderedRows.keys, err = e.encodeJoinKeys(e.outerOrderedRows.requestRows)
		if err != nil {
			return nil, errors.Trace(err)
		}

		sort.Sort(e.outerOrderedRows)

		requestRows := e.deDuplicateRequestRow(e.outerOrderedRows.requestRows, e.outerOrderedRows.keys)
		if err := e.fetchInner(requestRows); err != nil {
			return nil, errors.Trace(err)
		}
		if err := e.doMergeJoin(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	result = e.resultBuffer[e.resultCursor]
	e.resultCursor++
	return result, nil
}

func (e *IndexLookUpJoin) constructRequestRows(outerRows []Row) (requestRows [][]types.Datum, err error) {
	requestRows = make([][]types.Datum, 0, len(outerRows))
	joinDatums := make([]types.Datum, 0, len(outerRows)*len(e.outerKeys))
	for _, outerRow := range outerRows {
		for i, outerKey := range e.outerKeys {
			outerDatum, err := outerKey.Eval(outerRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
			joinKeyDatum, err := outerDatum.ConvertTo(e.ctx.GetSessionVars().StmtCtx, e.innerKeys[i].GetType())
			if err != nil {
				return nil, errors.Trace(err)
			}
			joinDatums = append(joinDatums, joinKeyDatum)
		}
		requestRows = append(requestRows, joinDatums)
		joinDatums = joinDatums[len(joinDatums):]
	}
	return requestRows, nil
}

func (e *IndexLookUpJoin) encodeJoinKeys(joinRows [][]types.Datum) (keys [][]byte, err error) {
	keys = make([][]byte, 0, len(joinRows))
	for _, joinRow := range joinRows {
		key, err := codec.EncodeKey(nil, joinRow...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// requestKeys must be ordered.
func (e *IndexLookUpJoin) deDuplicateRequestRow(requestRows [][]types.Datum, requestKeys [][]byte) [][]types.Datum {
	result := requestRows[:1:len(requestRows)]
	for i, length := 1, len(requestRows); i < length; i++ {
		if !bytes.Equal(requestKeys[i], requestKeys[i-1]) {
			result = append(result, requestRows[i])
		}
	}
	return result
}

// fetchInner will join the outer rows and inner rows and store them to resultBuffer.
func (e *IndexLookUpJoin) fetchInner(requestRows [][]types.Datum) (err error) {
	if err := e.innerExec.doRequestForDatums(requestRows, e.ctx.GoCtx()); err != nil {
		return errors.Trace(err)
	}

	e.innerOrderedRows.rows = e.innerOrderedRows.rows[:0]
	for {
		innerRow, err := e.innerExec.Next()
		if err != nil {
			return errors.Trace(err)
		} else if innerRow == nil {
			break
		}

		matched, err := expression.EvalBool(e.innerFilter, innerRow, e.ctx)
		if err != nil {
			return errors.Trace(err)
		} else if !matched {
			continue
		}

		e.innerOrderedRows.rows = append(e.innerOrderedRows.rows, innerRow)
	}

	innerJoinRows := make([][]types.Datum, 0, len(e.innerOrderedRows.rows))
	joinDatums := make([]types.Datum, 0, len(e.innerOrderedRows.rows)*len(e.innerKeys))
	for _, innerRow := range e.innerOrderedRows.rows {
		for _, innerKey := range e.innerKeys {
			innerDatum, err := innerKey.Eval(innerRow)
			if err != nil {
				return errors.Trace(err)
			}
			joinDatums = append(joinDatums, innerDatum)
		}
		innerJoinRows = append(innerJoinRows, joinDatums)
		joinDatums = joinDatums[len(joinDatums):]
	}

	e.innerOrderedRows.keys, err = e.encodeJoinKeys(innerJoinRows)
	if err != nil {
		return errors.Trace(err)
	}

	sort.Sort(e.innerOrderedRows)
	return nil
}

// getNextCursor will move cursor to the next datum that is different from the previous one and return it.
func getNextCursor(cursor int, orderedRows keyRowBlock) int {
	for cursor++; cursor < len(orderedRows.rows); cursor++ {
		if bytes.Compare(orderedRows.keys[cursor], orderedRows.keys[cursor-1]) != 0 {
			return cursor
		}
	}
	return cursor
}

// doMergeJoin joins the innerOrderedRows and outerOrderedRows which have been sorted before.
func (e *IndexLookUpJoin) doMergeJoin() (err error) {
	var outerCursor, innerCursor int
	for outerCursor < len(e.outerOrderedRows.rows) && innerCursor < len(e.innerOrderedRows.rows) {
		outerEndCursor := getNextCursor(outerCursor, e.outerOrderedRows)
		innerEndCursor := getNextCursor(innerCursor, e.innerOrderedRows)

		c := bytes.Compare(e.outerOrderedRows.keys[outerCursor], e.innerOrderedRows.keys[innerCursor])
		if c > 0 {
			innerCursor = innerEndCursor
			continue
		}

		if c < 0 {
			e.resultBuffer = e.resultGenerator.emitUnMatchedOuters(e.outerOrderedRows.rows[outerCursor:outerEndCursor], e.resultBuffer)
		} else {
			outerBeginCursor := outerCursor
			for i := outerBeginCursor; i < outerEndCursor; i++ {
				initLen := len(e.resultBuffer)
				outerRow := e.outerOrderedRows.rows[i]
				e.resultBuffer, err = e.resultGenerator.emitMatchedInners(outerRow, e.innerOrderedRows.rows[innerCursor:innerEndCursor], e.resultBuffer)
				if err != nil {
					return errors.Trace(err)
				}
				if initLen == len(e.resultBuffer) {
					e.resultBuffer = e.resultGenerator.emitUnMatchedOuter(outerRow, e.resultBuffer)
				}
			}
			innerCursor = innerEndCursor
		}
		outerCursor = outerEndCursor
	}
	e.resultBuffer = e.resultGenerator.emitUnMatchedOuters(e.outerOrderedRows.rows[outerCursor:], e.resultBuffer)
	return nil
}
