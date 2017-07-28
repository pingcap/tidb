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

type orderedRow struct {
	key []byte
	row *Row
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
	resultRows  []*Row
	outerRows   orderedRows
	innerRows   orderedRows
	innerDatums orderedRows // innerDatums are extracted by innerRows and innerJoinKeys
	exhausted   bool        // exhausted means whether all data has been extracted

	outerJoinKeys   []*expression.Column
	innerJoinKeys   []*expression.Column
	leftConditions  expression.CNFExprs
	rightConditions expression.CNFExprs
	otherConditions expression.CNFExprs
	defaultValues   []types.Datum
	outer           bool
	batchSize       int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpJoin) Open() error {
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
func (e *IndexLookUpJoin) Next() (*Row, error) {
	for e.cursor == len(e.resultRows) {
		if e.exhausted {
			return nil, nil
		}
		e.outerRows = e.outerRows[:0]
		e.resultRows = e.resultRows[:0]
		e.innerDatums = e.innerDatums[:0]
		for i := 0; i < e.batchSize; i++ {
			outerRow, err := e.children[0].Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if outerRow == nil {
				e.exhausted = true
				break
			}
			match, err := expression.EvalBool(e.leftConditions, outerRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if match {
				joinDatums := make([]types.Datum, 0, len(e.outerJoinKeys))
				for i, col := range e.outerJoinKeys {
					datum, err := col.Eval(outerRow.Data)
					if err != nil {
						return nil, errors.Trace(err)
					}
					innerDatum, err := datum.ConvertTo(e.ctx.GetSessionVars().StmtCtx, e.innerJoinKeys[i].GetType())
					if err != nil {
						return nil, errors.Trace(err)
					}
					joinDatums = append(joinDatums, innerDatum)
				}
				joinOuterEncodeKey, err := codec.EncodeValue(nil, joinDatums...)
				if err != nil {
					return nil, errors.Trace(err)
				}
				e.outerRows = append(e.outerRows, orderedRow{key: joinOuterEncodeKey, row: outerRow})
				e.innerDatums = append(e.innerDatums, orderedRow{key: joinOuterEncodeKey, row: &Row{Data: joinDatums}})
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

func (e *IndexLookUpJoin) fillDefaultValues(row *Row) *Row {
	row.Data = append(row.Data, e.defaultValues...)
	return row
}

func getUniqueDatums(rows orderedRows) [][]types.Datum {
	datums := make([][]types.Datum, 0, rows.Len())
	sort.Sort(rows)
	for i := range rows {
		if i > 0 && bytes.Compare(rows[i-1].key, rows[i].key) == 0 {
			continue
		}
		datums = append(datums, rows[i].row.Data)
	}
	return datums
}

// doJoin will join the outer rows and inner rows and store them to resultRows.
func (e *IndexLookUpJoin) doJoin() error {
	err := e.innerExec.doRequestForDatums(getUniqueDatums(e.innerDatums), e.ctx.GoCtx())
	if err != nil {
		return errors.Trace(err)
	}
	defer e.innerExec.Close()
	for {
		innerRow, err := e.innerExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if innerRow == nil {
			break
		}
		match, err := expression.EvalBool(e.rightConditions, innerRow.Data, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}
		joinDatums := make([]types.Datum, 0, len(e.innerJoinKeys))
		for _, col := range e.innerJoinKeys {
			datum, _ := col.Eval(innerRow.Data)
			joinDatums = append(joinDatums, datum)
		}
		joinKey, err := codec.EncodeValue(nil, joinDatums...)
		if err != nil {
			return errors.Trace(err)
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
					joinedRow := makeJoinRow(outerRow, innerRow)
					match, err := expression.EvalBool(e.otherConditions, joinedRow.Data, e.ctx)
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
		} else if c < 0 { // outer smaller then inner, move and enlarge outer cursor
			outerCursor = getNextCursor(outerCursor, e.outerRows)
			outerRow := e.outerRows[outerCursor].row
			if e.outer {
				e.resultRows = append(e.resultRows, e.fillDefaultValues(outerRow))
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
