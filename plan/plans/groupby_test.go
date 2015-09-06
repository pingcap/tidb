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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
)

type testGroupBySuite struct{}

var _ = Suite(&testGroupBySuite{})

var groupByTestData = []*testRowData{
	&testRowData{1, []interface{}{10, "10"}},
	&testRowData{2, []interface{}{10, "20"}},
	&testRowData{3, []interface{}{10, "30"}},
	&testRowData{4, []interface{}{40, "40"}},
	&testRowData{6, []interface{}{60, "60"}},
}

func (t *testGroupBySuite) TestGroupBy(c *C) {
	tblPlan := &testTablePlan{groupByTestData, []string{"id", "name"}}
	// test multiple fields
	sl := &SelectList{
		Fields: []*field.Field{
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("id"),
				},
			},
			&field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr("name"),
				},
			},
			&field.Field{
				Expr: &expressions.Call{
					F: "sum",
					Args: []expression.Expression{
						&expressions.Ident{
							CIStr: model.NewCIStr("id"),
						},
					},
				},
			},
		},
		AggFields: map[int]struct{}{2: struct{}{}},
	}

	groupbyPlan := &GroupByDefaultPlan{
		SelectList: sl,
		Src:        tblPlan,
		By: []expression.Expression{
			&expressions.Ident{
				CIStr: model.NewCIStr("id"),
			},
		},
	}

	ret := map[int]string{}
	groupbyPlan.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		ret[data[0].(int)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 3)
	excepted := map[int]string{
		10: "10",
		40: "40",
		60: "60",
	}
	c.Assert(ret, DeepEquals, excepted)

	// test empty
	tblPlan.rows = []*testRowData{}
	groupbyPlan.Src = tblPlan
	groupbyPlan.By = nil
	groupbyPlan.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		return true, nil
	})
}
