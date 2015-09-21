package mocks

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

// MockPlan mocks a plan.Plan instance
type MockPlan struct {
	rset   *Recordset
	rows   [][]interface{}
	cursor int
}

// NewMockPlan creates a new MockPlan
func NewMockPlan(rset *Recordset) *MockPlan {
	return &MockPlan{rset: rset}
}

// Explain implements plan.Plan Explain interface.
func (p *MockPlan) Explain(w format.Formatter) {}

// GetFields implements plan.Plan GetFields interface.
func (p *MockPlan) GetFields() []*field.ResultField {
	fields, _ := p.rset.Fields()
	return fields[:p.rset.offset]
}

// Filter implements plan.Plan Filter interface.
func (p *MockPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, false, nil
}

// Next implements plan.Plan Next interface.
func (p *MockPlan) Next(ctx context.Context) (row *plan.Row, err error) {
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

// Close implements plan.Plan Close interface{}
func (p *MockPlan) Close() error {
	p.cursor = 0
	return nil
}
