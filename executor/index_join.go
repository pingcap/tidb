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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

type orderedRow struct {
	key []byte
	row Row
}

type orderedRows []orderedRow

// Len returns the number of rows.
func (e orderedRows) Len() int {
	return len(e)
}

// Swap implements sort.Interface Swap interface.
func (e orderedRows) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Less implements sort.Interface Less interface.
func (e orderedRows) Less(i, j int) bool {
	return bytes.Compare(e[i].key, e[j].key) < 0
}

// IndexLookUpJoin fetches batches of data from outer executor and constructs ranges for inner executor.
type IndexLookUpJoin struct {
	baseExecutor

	innerExec DataReader

	cursor      int
	resultRows  []Row
	outerRows   orderedRows
	innerRows   orderedRows
	innerDatums orderedRows // innerDatums are extracted by innerRows and innerJoinKeys
	exhausted   bool        // exhausted means whether all data has been extracted

	leftIsOuter     bool
	outerJoinKeys   []*expression.Column
	innerJoinKeys   []*expression.Column
	outerConditions expression.CNFExprs
	innerConditions expression.CNFExprs
	otherConditions expression.CNFExprs
	defaultValues   []types.Datum
	outer           bool
	batchSize       int
	curBatchSize    int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpJoin) Open() error {
	e.curBatchSize = e.ctx.GetSessionVars().InitialIndexJoinBatchSize
	e.cursor = 0
	e.exhausted = false
	return errors.Trace(e.children[0].Open())
}

// Close implements the Executor Close interface.
func (e *IndexLookUpJoin) Close() error {
	e.resultRows = nil
	e.outerRows = nil
	e.innerDatums = nil
	return errors.Trace(e.children[0].Close())
}

// Next implements the Executor Next interface.
// We will fetch batches of row from outer executor, construct the inner datums and sort them.
// At the same time we will fetch the inner row by the inner datums and apply merge join.
func (e *IndexLookUpJoin) Next() (Row, error) {
	for e.cursor == len(e.resultRows) {
		if e.exhausted {
			return nil, nil
		}
		e.curBatchSize *= 2
		if e.curBatchSize > e.batchSize {
			e.curBatchSize = e.batchSize
		}
		e.outerRows = e.outerRows[:0]
		e.innerRows = e.innerRows[:0]
		e.resultRows = e.resultRows[:0]
		e.innerDatums = e.innerDatums[:0]
		for i := 0; i < e.curBatchSize; i++ {
			outerRow, err := e.children[0].Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if outerRow == nil {
				e.exhausted = true
				break
			}
			match, err := expression.EvalBool(e.outerConditions, outerRow, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if match {
				joinDatums := make([]types.Datum, 0, len(e.outerJoinKeys))
				for i, col := range e.outerJoinKeys {
					datum, err := col.Eval(outerRow)
					if err != nil {
						return nil, errors.Trace(err)
					}
					innerDatum, err := datum.ConvertTo(e.ctx.GetSessionVars().StmtCtx, e.innerJoinKeys[i].GetType())
					if err != nil {
						return nil, errors.Trace(err)
					}
					joinDatums = append(joinDatums, innerDatum)
				}
				joinOuterEncodeKey, err := codec.EncodeKey(nil, joinDatums...)
				if err != nil {
					return nil, errors.Trace(err)
				}
				e.outerRows = append(e.outerRows, orderedRow{key: joinOuterEncodeKey, row: outerRow})
				e.innerDatums = append(e.innerDatums, orderedRow{key: joinOuterEncodeKey, row: joinDatums})
			} else if e.outer {
				e.resultRows = append(e.resultRows, e.fillDefaultValues(outerRow))
			}
		}
		sort.Sort(e.outerRows)
		err := e.doJoin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.cursor = 0
	}
	row := e.resultRows[e.cursor]
	e.cursor++
	return row, nil
}

func (e *IndexLookUpJoin) fillDefaultValues(row Row) Row {
	if e.leftIsOuter {
		row = append(row, e.defaultValues...)
	} else {
		row = append(e.defaultValues, row...)
	}
	return row
}

func getUniqueDatums(rows orderedRows) [][]types.Datum {
	datums := make([][]types.Datum, 0, rows.Len())
	sort.Sort(rows)
	for i := range rows {
		if i > 0 && bytes.Equal(rows[i-1].key, rows[i].key) {
			continue
		}
		datums = append(datums, rows[i].row)
	}
	return datums
}

// doJoin will join the outer rows and inner rows and store them to resultRows.
func (e *IndexLookUpJoin) doJoin() error {
	err := e.innerExec.doRequestForDatums(getUniqueDatums(e.innerDatums), e.ctx.GoCtx())
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(e.innerExec.Close)
	for {
		innerRow, err1 := e.innerExec.Next()
		if err1 != nil {
			return errors.Trace(err1)
		}
		if innerRow == nil {
			break
		}
		match, err1 := expression.EvalBool(e.innerConditions, innerRow, e.ctx)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !match {
			continue
		}
		joinDatums := make([]types.Datum, 0, len(e.innerJoinKeys))
		for _, col := range e.innerJoinKeys {
			datum, err2 := col.Eval(innerRow)
			if err2 != nil {
				return errors.Trace(err2)
			}
			joinDatums = append(joinDatums, datum)
		}
		joinKey, err1 := codec.EncodeKey(nil, joinDatums...)
		if err1 != nil {
			return errors.Trace(err1)
		}
		e.innerRows = append(e.innerRows, orderedRow{key: joinKey, row: innerRow})
	}
	sort.Sort(e.innerRows)
	return e.doMergeJoin()
}

// getNextCursor will move cursor to the next datum that is different from the previous one and return it.
func getNextCursor(cursor int, rows orderedRows) int {
	for {
		cursor++
		if cursor >= len(rows) {
			break
		}
		c := bytes.Compare(rows[cursor].key, rows[cursor-1].key)
		if c != 0 {
			break
		}
	}
	return cursor
}

// doMergeJoin joins the innerRows and outerRows which have been sorted before.
func (e *IndexLookUpJoin) doMergeJoin() error {
	var outerCursor, innerCursor int
	for outerCursor < len(e.outerRows) && innerCursor < len(e.innerRows) {
		c := bytes.Compare(e.outerRows[outerCursor].key, e.innerRows[innerCursor].key)
		if c == 0 {
			outerBeginCursor := outerCursor
			outerEndCursor := getNextCursor(outerCursor, e.outerRows)
			innerBeginCursor := innerCursor
			innerEndCursor := getNextCursor(innerCursor, e.innerRows)
			for i := outerBeginCursor; i < outerEndCursor; i++ {
				var outerMatch bool
				outerRow := e.outerRows[i].row
				for j := innerBeginCursor; j < innerEndCursor; j++ {
					innerRow := e.innerRows[j].row
					var joinedRow Row
					if e.leftIsOuter {
						joinedRow = makeJoinRow(outerRow, innerRow)
					} else {
						joinedRow = makeJoinRow(innerRow, outerRow)
					}
					match, err := expression.EvalBool(e.otherConditions, joinedRow, e.ctx)
					if err != nil {
						return errors.Trace(err)
					}
					if match {
						outerMatch = true
						e.resultRows = append(e.resultRows, joinedRow)
					}
				}
				if e.outer && !outerMatch {
					e.resultRows = append(e.resultRows, e.fillDefaultValues(outerRow))
				}
			}
			outerCursor, innerCursor = outerEndCursor, innerEndCursor
		} else if c < 0 {
			// If outer smaller than inner, move and enlarge outer cursor
			nextOuterCursor := getNextCursor(outerCursor, e.outerRows)
			if !e.outer {
				outerCursor = nextOuterCursor
			} else {
				for outerCursor < nextOuterCursor {
					outerRow := e.outerRows[outerCursor].row
					e.resultRows = append(e.resultRows, e.fillDefaultValues(outerRow))
					outerCursor++
				}
			}
		} else {
			innerCursor = getNextCursor(innerCursor, e.outerRows)
		}
	}
	for e.outer && outerCursor < len(e.outerRows) {
		outerRow := e.outerRows[outerCursor].row
		e.resultRows = append(e.resultRows, e.fillDefaultValues(outerRow))
		outerCursor++
	}
	return nil
}
