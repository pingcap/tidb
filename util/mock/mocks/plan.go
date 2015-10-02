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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

// Plan represents mocked plan.Plan.
type Plan struct {
	rset   *Recordset
	rows   [][]interface{}
	cursor int
}

// NewPlan creates a new mocked plan.Plan.
func NewPlan(rset *Recordset) *Plan {
	return &Plan{rset: rset}
}

// Explain implements plan.Plan Explain interface.
func (p *Plan) Explain(w format.Formatter) {}

// GetFields implements plan.Plan GetFields interface.
func (p *Plan) GetFields() []*field.ResultField {
	fields, _ := p.rset.Fields()
	return fields[:p.rset.offset]
}

// Filter implements plan.Plan Filter interface.
func (p *Plan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, false, nil
}

// Next implements plan.Plan Next interface.
func (p *Plan) Next(ctx context.Context) (row *plan.Row, err error) {
	if p.rows == nil {
		p.rows, _ = p.rset.Rows(-1, 0)
	}
	if p.cursor == len(p.rows) {
		return
	}
	row = &plan.Row{Data: p.rows[p.cursor]}
	p.cursor++
	return
}

// Close implements plan.Plan Close interface.
func (p *Plan) Close() error {
	p.cursor = 0
	return nil
}
