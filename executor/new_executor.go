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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
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
	//TODO: remove fields when abandon old plan.
	fields      []*ast.ResultField
	outter      bool
	leftSmall   bool
	matchedRows []*Row
	cursor      int
}

func joinTwoRow(a *Row, b *Row) *Row {
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

func (e *HashJoinExec) getHashKey(exprs []*expression.Column, row *Row) ([]byte, error) {
	vals := make([]types.Datum, 0, len(exprs))
	for _, expr := range exprs {
		v, err := expr.Eval(row.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		return []byte{}, nil
	}

	return codec.EncodeValue([]byte{}, vals...)
}

// Fields implements Executor Fields interface.
func (e *HashJoinExec) Fields() []*ast.ResultField {
	return e.fields
}

// Close implements Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.hashTable = nil
	e.matchedRows = nil
	return nil
}

func (e *HashJoinExec) prepare() error {
	e.hashTable = make(map[string][]*Row)
	e.cursor = 0
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
		hashcode, err := e.getHashKey(e.smallHashKey, row)
		if err != nil {
			return errors.Trace(err)
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

func (e *HashJoinExec) constructMatchedRows(bigRow *Row) (matchedRows []*Row, err error) {
	hashcode, err := e.getHashKey(e.bigHashKey, bigRow)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		//TODO: remove result fields in order to reduce memory copy cost.
		otherMatched := true
		if e.otherFilter != nil {
			startKey := 0
			if !e.leftSmall {
				startKey = len(bigRow.Data)
			}
			for i, data := range smallRow.Data {
				e.fields[i+startKey].Expr.SetValue(data.GetValue())
			}
			otherMatched, err = expression.EvalBool(e.otherFilter, bigRow.Data, e.ctx)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if otherMatched {
			if e.leftSmall {
				matchedRows = append(matchedRows, joinTwoRow(smallRow, bigRow))
			} else {
				matchedRows = append(matchedRows, joinTwoRow(bigRow, smallRow))
			}
		}
	}

	return matchedRows, nil
}

func (e *HashJoinExec) fillNullRow(bigRow *Row) (returnRow *Row, err error) {
	smallRow := &Row{
		RowKeys: make([]*RowKeyEntry, len(e.smallExec.Fields())),
		Data:    make([]types.Datum, len(e.smallExec.Fields())),
	}

	for _, data := range smallRow.Data {
		data.SetNull()
	}
	if e.leftSmall {
		returnRow = joinTwoRow(smallRow, bigRow)
	} else {
		returnRow = joinTwoRow(bigRow, smallRow)
	}
	for i, data := range returnRow.Data {
		e.fields[i].Expr.SetValue(data.GetValue())
	}
	return returnRow, nil
}

func (e *HashJoinExec) returnRecord() (ret *Row, ok bool) {
	if e.cursor >= len(e.matchedRows) {
		return nil, false
	}
	for i, data := range e.matchedRows[e.cursor].Data {
		e.fields[i].Expr.SetValue(data.GetValue())
	}
	e.cursor++
	return e.matchedRows[e.cursor-1], true
}

// Next implements Executor Next interface.
func (e *HashJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	row, ok := e.returnRecord()
	if ok {
		return row, nil
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

		var matchedRows []*Row
		bigMatched := true
		if e.bigFilter != nil {
			bigMatched, err = expression.EvalBool(e.bigFilter, row.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if bigMatched {
			matchedRows, err = e.constructMatchedRows(bigRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		e.matchedRows = matchedRows
		e.cursor = 0
		row, ok := e.returnRecord()
		if ok {
			return row, nil
		} else if e.outter {
			return e.fillNullRow(bigRow)
		}
	}
}
