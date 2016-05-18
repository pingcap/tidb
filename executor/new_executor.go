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
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// HashJoinExec implements the hash join algorithm
type HashJoinExec struct {
	hashTable    map[string][]*Row
	smallHashKey []ast.ExprNode
	bigHashKey   []ast.ExprNode
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	fields       []*ast.ResultField
	ctx          context.Context
	smallFilter  ast.ExprNode
	bigFilter    ast.ExprNode
	otherFilter  ast.ExprNode
	outter       bool
	leftSmall    bool
}

func joinTwoRow(a *Row, b *Row) *Row {
	return &Row{RowKeys: append(a.RowKeys, b.RowKeys...), Data: append(a.Data, b.Data...)}
}

func (e *HashJoinExec) getHashKey(exprs []ast.ExprNode) ([]byte, error) {
	vals := make([]types.Datum, 0, len(exprs))
	for _, expr := range exprs {
		v, err := evaluator.Eval(e.ctx, expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		return []byte{}, nil
	}
	result, err := codec.EncodeValue([]byte{}, vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, err
}

// Fields implements Executor Fields interface
func (e *HashJoinExec) Fields() []*ast.ResultField {
	return e.fields
}

// Close implements Executor Close interface
func (e *HashJoinExec) Close() error {
	e.hashTable = nil
	return nil
}

func (e *HashJoinExec) prepare() error {
	e.hashTable = make(map[string][]*Row)
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			e.prepared = true
			break
		}
		matched := true
		if e.smallFilter != nil {
			matched, err = evaluator.EvalBool(e.ctx, e.smallFilter)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		hashcode, err := e.getHashKey(e.smallHashKey)
		if err != nil {
			return err
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

func (e *HashJoinExec) constructMatchedRow(bigRow *Row) (matchedRows []*Row, err error) {
	hashcode, err := e.getHashKey(e.bigHashKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// match eq condition
	if rows, ok := e.hashTable[string(hashcode)]; ok {
		for _, smallRow := range rows {
			//TODO: remove result fields in order to reduce memory copy cost.
			startKey := 0
			if !e.leftSmall {
				startKey = len(bigRow.Data)
			}
			for i, data := range smallRow.Data {
				e.fields[i+startKey].Expr.SetValue(data.GetValue())
			}
			otherMatched := true
			if e.otherFilter != nil {
				otherMatched, err = evaluator.EvalBool(e.ctx, e.otherFilter)
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

// Next implements Executor Next interface
func (e *HashJoinExec) Next() (*Row, error) {
	if !e.prepared {
		err := e.prepare()
		if err != nil {
			return nil, err
		}
	}
	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, err
		}
		if bigRow == nil {
			e.bigExec.Close()
			return nil, nil
		}
		var matchedRows []*Row
		bigMatched := true
		if e.bigFilter != nil {
			bigMatched, err = evaluator.EvalBool(e.ctx, e.bigFilter)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if bigMatched {
			matchedRows, err = e.constructMatchedRow(bigRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		for _, row := range matchedRows {
			for i, data := range row.Data {
				e.fields[i].Expr.SetValue(data.GetValue())
			}
			return row, nil
		}
		if e.outter && len(matchedRows) == 0 {
			return e.fillNullRow(bigRow)
		}
	}
}
