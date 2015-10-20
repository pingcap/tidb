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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
)

var _ = Suite(&testOrderByRsetSuite{})

type testOrderByRsetSuite struct {
	r *OrderByRset
}

func (s *testOrderByRsetSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)
	resultFields := tblPlan.GetFields()

	fields := make([]*field.Field, len(resultFields))
	for i, resultField := range resultFields {
		name := resultField.Name
		fields[i] = &field.Field{Expr: &expression.Ident{CIStr: model.NewCIStr(name)}}
	}

	selectList := &plans.SelectList{
		HiddenFieldOffset: len(resultFields),
		ResultFields:      resultFields,
		Fields:            fields,
	}

	s.r = &OrderByRset{Src: tblPlan, SelectList: selectList}
}

func (s *testOrderByRsetSuite) TestOrderByRsetPlan(c *C) {
	// `select id, name from t`
	_, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t order by id`
	expr := &expression.Ident{CIStr: model.NewCIStr("id")}
	orderByItem := OrderByItem{Expr: expr}
	by := []OrderByItem{orderByItem}

	s.r.By = by

	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.OrderByDefaultPlan)
	c.Assert(ok, IsTrue)

	// `select id, name from t order by 1`
	s.r.By[0].Expr = expression.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	s.r.By[0].Expr = expression.Value{Val: uint64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t order by 0`
	s.r.By[0].Expr = expression.Value{Val: int64(0)}

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	s.r.By[0].Expr = expression.Value{Val: 0}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// check src plan is NullPlan
	s.r.Src = &plans.NullPlan{Fields: s.r.SelectList.ResultFields}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)
}

func (s *testOrderByRsetSuite) TestOrderByRsetString(c *C) {
	str := s.r.String()
	c.Assert(len(str), Greater, 0)

	s.r.By[0].Asc = true
	str = s.r.String()
	c.Assert(len(str), Greater, 0)

	s.r.By = nil
	str = s.r.String()
	c.Assert(len(str), Equals, 0)
}
