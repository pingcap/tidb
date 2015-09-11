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

package expressions

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type mockExpr struct {
	isStatic bool
	val      interface{}
	err      error
}

func (m mockExpr) IsStatic() bool {
	return m.isStatic
}

func (m mockExpr) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return m.val, m.err
}

func (m mockExpr) String() string {
	return "mock expression"
}

func (m mockExpr) Clone() (expression.Expression, error) {
	nm := m
	return nm, m.err
}

type mockCtx struct {
	vars map[fmt.Stringer]interface{}
}

func newMockCtx() *mockCtx {
	return &mockCtx{vars: make(map[fmt.Stringer]interface{})}
}

func (c *mockCtx) GetTxn(forceNew bool) (kv.Transaction, error) { return nil, nil }

func (c *mockCtx) FinishTxn(rollback bool) error { return nil }

func (c *mockCtx) SetValue(key fmt.Stringer, value interface{}) {
	c.vars[key] = value
}

func (c *mockCtx) Value(key fmt.Stringer) interface{} {
	return c.vars[key]
}

func (c *mockCtx) ClearValue(key fmt.Stringer) {
	delete(c.vars, key)
}

type mockRecordset struct {
	rows   [][]interface{}
	fields []string
	offset int
}

func newMockRecordset() *mockRecordset {
	return &mockRecordset{
		rows: [][]interface{}{
			{1, 1},
			{2, 2},
		},
		fields: []string{"id", "value"},
		offset: 2,
	}
}

func (r *mockRecordset) Do(f func(data []interface{}) (more bool, err error)) error {
	for i := range r.rows {
		if more, err := f(r.rows[i]); !more || err != nil {
			return err
		}
	}
	return nil
}

func (r *mockRecordset) Fields() ([]*field.ResultField, error) {
	var ret []*field.ResultField
	for _, fn := range r.fields {
		resultField := &field.ResultField{Name: fn, TableName: "t"}
		resultField.Col.Name = model.NewCIStr(fn)
		ret = append(ret, resultField)
	}

	return ret[:r.offset], nil
}

func (r *mockRecordset) FirstRow() ([]interface{}, error) {
	return r.rows[0], nil
}

func (r *mockRecordset) Rows(limit, offset int) ([][]interface{}, error) {
	var ret [][]interface{}
	for _, row := range r.rows {
		ret = append(ret, row[:r.offset])
	}

	return ret, nil
}

func (r *mockRecordset) SetFieldOffset(offset int) {
	r.offset = offset
}

type mockStatement struct {
	text string
	rset *mockRecordset
	plan *mockPlan
}

func newMockStatement() *mockStatement {
	ms := &mockStatement{rset: newMockRecordset()}
	ms.plan = newMockPlan(ms.rset)
	return ms
}

func (s *mockStatement) Exec(ctx context.Context) (rset.Recordset, error) {
	return s.rset, nil
}

func (s *mockStatement) Explain(ctx context.Context, w format.Formatter) {
}

func (s *mockStatement) IsDDL() bool {
	return false
}

func (s *mockStatement) OriginText() string {
	return s.text
}

func (s *mockStatement) SetText(text string) {
	s.text = text
}

func (s *mockStatement) Plan(ctx context.Context) (plan.Plan, error) {
	return s.plan, nil
}

func (s *mockStatement) SetFieldOffset(offset int) {
	s.rset.SetFieldOffset(offset)
}

func (s *mockStatement) String() string {
	return "mock statement"
}

type mockPlan struct {
	rset *mockRecordset
}

func newMockPlan(rset *mockRecordset) *mockPlan {
	return &mockPlan{rset: rset}
}

func (p *mockPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	for _, data := range p.rset.rows {

		if more, err := f(nil, data[:p.rset.offset]); !more || err != nil {
			return err
		}
	}
	return nil
}

func (p *mockPlan) Explain(w format.Formatter) {}

func (p *mockPlan) GetFields() []*field.ResultField {
	fields, _ := p.rset.Fields()
	return fields[:p.rset.offset]
}

func (p *mockPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, false, nil
}
