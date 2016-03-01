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

package mocks

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
)

// Recordset represents mocked ast.RecordSet.
type Recordset struct {
	rows   [][]interface{}
	fields []string
	offset int
	cursor int
}

// NewRecordset creates a new mocked rset.Recordset.
func NewRecordset(rows [][]interface{}, fields []string, offset int) *Recordset {
	return &Recordset{
		rows:   rows,
		fields: fields,
		offset: offset,
	}
}

// Fields implements rset.Recordset Fields interface.
func (r *Recordset) Fields() ([]*ast.ResultField, error) {
	var ret []*ast.ResultField
	for _, fn := range r.fields {
		resultField := &ast.ResultField{
			ColumnAsName: model.NewCIStr(fn),
			TableName: &ast.TableName{
				Name: model.NewCIStr("t"),
			},
		}
		ret = append(ret, resultField)
	}

	return ret[:r.offset], nil
}

// SetFieldOffset sets field offset.
func (r *Recordset) SetFieldOffset(offset int) {
	r.offset = offset
}

// Next implements rset.Recordset Next interface.
func (r *Recordset) Next() (row *ast.Row, err error) {
	if r.cursor == len(r.rows) {
		return
	}
	row = &ast.Row{Data: r.rows[r.cursor]}
	r.cursor++
	return
}

// Close implements rset.Recordset Close interface.
func (r *Recordset) Close() error {
	r.cursor = 0
	return nil
}
