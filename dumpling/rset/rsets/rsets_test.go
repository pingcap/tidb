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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testRowData struct {
	id   int64
	data []interface{}
}

type testTablePlan struct {
	rows   []*testRowData
	fields []string
	filter bool
	retNil bool
	cursor int
}

var testData = []*testRowData{
	{1, []interface{}{10, "hello"}},
	{2, []interface{}{10, "hello"}},
	{3, []interface{}{10, "hello"}},
	{4, []interface{}{40, "hello"}},
	{6, []interface{}{60, "hello"}},
}

func newTestTablePlan(rows []*testRowData, fields []string) *testTablePlan {
	return &testTablePlan{rows: rows, fields: fields}
}

func (p *testTablePlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	if p.retNil {
		return nil
	}

	for _, d := range p.rows {
		if more, err := f(d.id, d.data); !more || err != nil {
			return err
		}
	}
	return nil
}

func (p *testTablePlan) Explain(w format.Formatter) {}

func (p *testTablePlan) GetFields() []*field.ResultField {
	var ret []*field.ResultField
	for _, fn := range p.fields {
		resultField := &field.ResultField{Name: fn, TableName: "t"}
		resultField.Col.Name = model.NewCIStr(fn)
		ret = append(ret, resultField)
	}
	return ret
}

func (p *testTablePlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, p.filter, nil
}

func (p *testTablePlan) SetFilter(filter bool) {
	p.filter = filter
}

func (p *testTablePlan) SetRetNil(retNil bool) {
	p.retNil = retNil
}

func (p *testTablePlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if p.cursor == len(p.rows) || p.retNil {
		return
	}
	row = &plan.Row{Data: p.rows[p.cursor].data}
	p.cursor++
	return
}

func (p *testTablePlan) Close() error {
	p.cursor = 0
	return nil
}

var _ = Suite(&testRsetsSuite{})

type testRsetsSuite struct {
	r     Recordset
	names []string
}

func (s *testRsetsSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)

	s.r = Recordset{Ctx: nil, Plan: tblPlan}
	s.names = names
}

func (s *testRsetsSuite) TestGetFields(c *C) {
	fields := s.r.GetFields()
	c.Assert(fields, HasLen, 2)

	for i, fld := range fields {
		v, ok := fld.(*field.ResultField)
		c.Assert(ok, IsTrue)
		c.Assert(v.Name, Equals, s.names[i])
	}
}

func (s *testRsetsSuite) TestDo(c *C) {
	var checkDatas [][]interface{}
	f := func(data []interface{}) (bool, error) {
		checkDatas = append(checkDatas, data)
		return true, nil
	}

	err := s.r.Do(f)
	c.Assert(err, IsNil)

	c.Assert(checkDatas, HasLen, 5)
	for i, v := range checkDatas {
		c.Assert(v, HasLen, 2)
		c.Assert(v, DeepEquals, testData[i].data)
	}
}

func (s *testRsetsSuite) TestFields(c *C) {
	fields, err := s.r.Fields()
	c.Assert(fields, HasLen, 2)
	c.Assert(err, IsNil)

	for i, fld := range fields {
		c.Assert(fld.Name, Equals, s.names[i])
	}
}

func (s *testRsetsSuite) TestFirstRow(c *C) {
	row, err := s.r.FirstRow()
	c.Assert(row, HasLen, 2)
	c.Assert(err, IsNil)
	c.Assert(row, DeepEquals, testData[0].data)

	src := s.r.Plan.(*testTablePlan)
	src.SetRetNil(true)

	row, err = s.r.FirstRow()
	c.Assert(row, HasLen, 0)
	c.Assert(err, IsNil)

	src.SetRetNil(false)
}

func (s *testRsetsSuite) TestRows(c *C) {
	rows, err := s.r.Rows(2, 1)
	c.Assert(rows, HasLen, 2)
	c.Assert(err, IsNil)

	for i, row := range rows {
		c.Assert(row, HasLen, 2)
		c.Assert(row, DeepEquals, testData[i+1].data)
	}

	rows, err = s.r.Rows(-1, 1)
	c.Assert(rows, HasLen, 4)
	c.Assert(err, IsNil)

	rows, err = s.r.Rows(0, 1)
	c.Assert(rows, HasLen, 0)
	c.Assert(err, IsNil)
}
