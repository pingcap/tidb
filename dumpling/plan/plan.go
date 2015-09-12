// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package plan

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/format"
)

// RowIterFunc is the callback for iterating records.
type RowIterFunc func(id interface{}, data []interface{}) (more bool, err error)

// Plan is the interface of query execution plan.
type Plan interface {
	// Do iterates records and applies plan logic to the result set.
	// TODO: only get id or some fields.
	Do(ctx context.Context, f RowIterFunc) error
	// Explain the plan.
	Explain(w format.Formatter)
	// GetFields returns the result field list for a plan.
	GetFields() []*field.ResultField
	// Filter try to use index plan to reduce the result set.
	// If index can be used, a new index plan is returned, 'filtered' is true.
	// If no index can be used, the original plan is returned and 'filtered' return false.
	Filter(ctx context.Context, expr expression.Expression) (p Plan, filtered bool, err error)

	// Next returns the next row of data and rowKeys, nil data means there is no more data to return.
	// Aggregation plan will fetch all the data at the first call.
	Next(ctx context.Context) (row *Row, err error)

	// Close closes the underlying iterator of the plan.
	Close() error
}

// NextPlan is the interface for plans that has implemented next.
type NextPlan interface {
	ImplementedNext() bool
}

// ImplementedNext checks if p has implemented Next method.
func ImplementedNext(p Plan) bool {
	if np, ok := p.(NextPlan); ok {
		return np.ImplementedNext()
	}
	return false
}

// Planner is implemented by any structure that has a Plan method.
type Planner interface {
	// Plan function returns Plan.
	Plan(ctx context.Context) (Plan, error)
}

// Row represents a record row.
type Row struct {
	Data    []interface{}
	RowKeys []*RowKeyEntry
}

// RowKeyEntry is designed for Delete statement in multi-table mode,
// we should know which table this row comes from
type RowKeyEntry struct {
	// The table which this row come from
	Tbl table.Table
	// Row handle
	Key string
}
