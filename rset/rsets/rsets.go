// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

package rsets

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
)

// Recordset implements rset.Recordset interface.
type Recordset struct {
	Ctx context.Context
	plan.Plan
}

// GetFields implements rset.Recordset.
func (r Recordset) GetFields() []interface{} {
	f := r.Plan.GetFields()
	a := make([]interface{}, len(f))
	for i, v := range f {
		a[i] = v
	}
	return a
}

// Do implements rset.Recordset.
func (r Recordset) Do(f func(data []interface{}) (bool, error)) error {
	return r.Plan.Do(r.Ctx, func(ID interface{}, data []interface{}) (bool, error) {
		return f(data)
	})
}

// Fields implements rset.Recordset.
func (r Recordset) Fields() (fields []*field.ResultField, err error) {
	return r.Plan.GetFields(), nil
}

// FirstRow implements rset.Recordset.
func (r Recordset) FirstRow() (row []interface{}, err error) {
	rows, err := r.Rows(1, 0)
	if err != nil {
		return nil, err
	}

	if len(rows) != 0 {
		return rows[0], nil
	}

	return nil, nil
}

// Rows implements rset.Recordset.
func (r Recordset) Rows(limit, offset int) ([][]interface{}, error) {
	var rows [][]interface{}
	err := r.Do(func(row []interface{}) (bool, error) {
		if offset > 0 {
			offset--
			return true, nil
		}

		switch {
		case limit < 0:
			rows = append(rows, row)
			return true, nil
		case limit == 0:
			return false, nil
		default: // limit > 0
			rows = append(rows, row)
			limit--
			return limit > 0, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}
