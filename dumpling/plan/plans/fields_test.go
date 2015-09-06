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

package plans

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
)

type testFieldsSuit struct{}

var _ = Suite(&testFieldsSuit{})

var selectFieldsTestData = []*testRowData{
	&testRowData{1, []interface{}{10, "hello"}},
	&testRowData{2, []interface{}{20, "hello"}},
	&testRowData{3, []interface{}{30, "hello"}},
	&testRowData{4, []interface{}{40, "hello"}},
	&testRowData{5, []interface{}{50, "hello"}},
	&testRowData{6, []interface{}{60, "hello"}},
}

func (s *testFieldsSuit) TestDefaultFieldsPlan(c *C) {
	tblPlan := &testTablePlan{selectFieldsTestData, []string{"id", "name"}}

	sl1 := &SelectList{
		Fields: []*field.Field{
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("name"),
				},
			},
		},
	}

	selFieldsPlan := &SelectFieldsDefaultPlan{
		SelectList: sl1,
		Src:        tblPlan,
	}

	selFieldsPlan.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		c.Assert(len(data), Equals, 1)
		c.Assert(data[0].(string), Equals, "hello")
		return true, nil
	})

	// test multiple fields
	sl2 := &SelectList{
		Fields: []*field.Field{
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("name"),
				},
			},
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("id"),
				},
			},
		},
	}

	selFieldsPlan = &SelectFieldsDefaultPlan{
		SelectList: sl2,
		Src:        tblPlan,
	}

	selFieldsPlan.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		c.Assert(len(data), Equals, 2)
		c.Assert(data[0].(string), Equals, "hello")
		c.Assert(data[1].(int), GreaterEqual, 10)
		return true, nil
	})

	// test field doesn't exists
	sl3 := &SelectList{
		Fields: []*field.Field{
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("nosuchfield"),
				},
			},
		},
	}
	selFieldsPlan = &SelectFieldsDefaultPlan{
		SelectList: sl3,
		Src:        tblPlan,
	}
	err := selFieldsPlan.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		return true, nil
	})
	c.Assert(err.Error() == "unknown field nosuchfield", Equals, true)
}

func (s *testFieldsSuit) TestSelectExprPlan(c *C) {
	pln := &SelectEmptyFieldListPlan{
		Fields: []*field.Field{
			&field.Field{
				Expr: &expressions.Value{
					Val: "data",
				},
			},
		},
	}
	fields := pln.GetFields()
	c.Assert(fields, HasLen, 1)
	pln.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		return true, nil
	})
}
