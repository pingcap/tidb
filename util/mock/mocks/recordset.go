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
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
)

// Recordset represents mocked rset.Recordset.
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

// Do implements rset.Recordset Do interface.
func (r *Recordset) Do(f func(data []interface{}) (more bool, err error)) error {
	for i := range r.rows {
		if more, err := f(r.rows[i]); !more || err != nil {
			return err
		}
	}
	return nil
}

// Fields implements rset.Recordset Fields interface.
func (r *Recordset) Fields() ([]*field.ResultField, error) {
	var ret []*field.ResultField
	for _, fn := range r.fields {
		resultField := &field.ResultField{Name: fn, TableName: "t"}
		resultField.Col.Name = model.NewCIStr(fn)
		ret = append(ret, resultField)
	}

	return ret[:r.offset], nil
}

// FirstRow implements rset.Recordset FirstRow interface.
func (r *Recordset) FirstRow() ([]interface{}, error) {
	return r.rows[0], nil
}

// Rows implements rset.Recordset Rows interface.
func (r *Recordset) Rows(limit, offset int) ([][]interface{}, error) {
	var ret [][]interface{}
	for _, row := range r.rows {
		ret = append(ret, row[:r.offset])
	}

	return ret, nil
}

// SetFieldOffset sets field offset.
func (r *Recordset) SetFieldOffset(offset int) {
	r.offset = offset
}

// Next implements rset.Recordset Next interface.
func (r *Recordset) Next() (row *plan.Row, err error) {
	if r.cursor == len(r.rows) {
		return
	}
	row = &plan.Row{Data: r.rows[r.cursor]}
	r.cursor++
	return
}

// Close implements rset.Recordset Close interface.
func (r *Recordset) Close() error {
	r.cursor = 0
	return nil
}
