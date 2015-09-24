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

package plans_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
)

type testFieldsSuit struct{}

var _ = Suite(&testFieldsSuit{})

var selectFieldsTestData = []*testRowData{
	{1, []interface{}{10, "hello"}},
	{2, []interface{}{20, "hello"}},
	{3, []interface{}{30, "hello"}},
	{4, []interface{}{40, "hello"}},
	{5, []interface{}{50, "hello"}},
	{6, []interface{}{60, "hello"}},
}

func (s *testFieldsSuit) TestDefaultFieldsPlan(c *C) {
	tblPlan := &testTablePlan{selectFieldsTestData, []string{"id", "name"}, 0}

	sl1 := &plans.SelectList{
		Fields: []*field.Field{
			{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr("name"),
				},
			},
		},
	}

	selFieldsPlan := &plans.SelectFieldsDefaultPlan{
		SelectList: sl1,
		Src:        tblPlan,
	}
	rset := rsets.Recordset{
		Plan: selFieldsPlan,
		Ctx:  mock.NewContext(),
	}
	rset.Do(func(data []interface{}) (bool, error) {
		c.Assert(len(data), Equals, 1)
		c.Assert(data[0].(string), Equals, "hello")
		return true, nil
	})

	// test multiple fields
	sl2 := &plans.SelectList{
		Fields: []*field.Field{
			{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr("name"),
				},
			},
			{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr("id"),
				},
			},
		},
	}
	tblPlan.Close()
	rset.Plan = &plans.SelectFieldsDefaultPlan{
		SelectList: sl2,
		Src:        tblPlan,
	}

	rset.Do(func(data []interface{}) (bool, error) {
		c.Assert(len(data), Equals, 2)
		c.Assert(data[0].(string), Equals, "hello")
		c.Assert(data[1].(int), GreaterEqual, 10)
		return true, nil
	})

	// test field doesn't exists
	sl3 := &plans.SelectList{
		Fields: []*field.Field{
			{
				Expr: &expression.Ident{
					CIStr: model.NewCIStr("nosuchfield"),
				},
			},
		},
	}
	tblPlan.Close()
	rset.Plan = &plans.SelectFieldsDefaultPlan{
		SelectList: sl3,
		Src:        tblPlan,
	}
	err := rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
	c.Assert(err.Error() == "unknown field nosuchfield", Equals, true)
}

func (s *testFieldsSuit) TestSelectExprPlan(c *C) {
	pln := &plans.SelectFromDualPlan{
		Fields: []*field.Field{
			{
				Expr: &expression.Value{
					Val: "data",
				},
			},
		},
	}
	fields := pln.GetFields()
	c.Assert(fields, HasLen, 1)
	rset := rsets.Recordset{
		Plan: pln,
		Ctx:  mock.NewContext()}
	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
}
