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

type testGroupBySuite struct{}

var _ = Suite(&testGroupBySuite{})

var groupByTestData = []*testRowData{
	{1, []interface{}{10, "10"}},
	{2, []interface{}{10, "20"}},
	{3, []interface{}{10, "30"}},
	{4, []interface{}{40, "40"}},
	{6, []interface{}{60, "60"}},
}

func (t *testGroupBySuite) TestGroupBy(c *C) {
	tblPlan := &testTablePlan{groupByTestData, []string{"id", "name"}, 0}
	// test multiple fields
	sl := &plans.SelectList{
		Fields: []*field.Field{
			{
				Expr: &expression.Ident{
					CIStr:      model.NewCIStr("id"),
					ReferScope: expression.IdentReferFromTable,
					ReferIndex: 0,
				},
			},
			{
				Expr: &expression.Ident{
					CIStr:      model.NewCIStr("name"),
					ReferScope: expression.IdentReferFromTable,
					ReferIndex: 1,
				},
			},
			{
				Expr: &expression.Call{
					F: "sum",
					Args: []expression.Expression{
						&expression.Ident{
							CIStr:      model.NewCIStr("id"),
							ReferScope: expression.IdentReferFromTable,
							ReferIndex: 0,
						},
					},
				},
			},
		},
		AggFields: map[int]struct{}{2: {}},
	}

	groupbyPlan := &plans.GroupByDefaultPlan{
		SelectList: sl,
		Src:        tblPlan,
		By: []expression.Expression{
			&expression.Ident{
				CIStr:      model.NewCIStr("id"),
				ReferScope: expression.IdentReferFromTable,
				ReferIndex: 0,
			},
		},
	}

	ret := map[int]string{}
	rset := rsets.Recordset{
		Plan: groupbyPlan,
		Ctx:  mock.NewContext(),
	}
	rset.Do(func(data []interface{}) (bool, error) {
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
	tblPlan.Close()
	groupbyPlan.Src = tblPlan
	groupbyPlan.By = nil
	groupbyPlan.Close()
	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
}
