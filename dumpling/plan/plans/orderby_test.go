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
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
)

type testOrderBySuit struct {
	data []*testRowData
}

var _ = Suite(&testOrderBySuit{
	[]*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	},
})

func (t *testOrderBySuit) TestOrderBy(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}, 0}

	pln := &plans.OrderByDefaultPlan{
		SelectList: &plans.SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
			ResultFields:      tblPlan.GetFields(),
		},
		Src: tblPlan,
		By: []expression.Expression{
			&expression.Ident{
				CIStr:      model.NewCIStr("id"),
				ReferScope: expression.IdentReferSelectList,
				ReferIndex: 0,
			},
		},
		Ascs: []bool{false},
	}

	prev := 10000
	rset := rsets.Recordset{
		Plan: pln,
		Ctx:  mock.NewContext(),
	}
	err := rset.Do(func(data []interface{}) (bool, error) {
		// DESC
		if data[0].(int) > prev {
			c.Error("should no be here", data[0], prev)
		}
		prev = data[0].(int)
		return true, nil
	})
	if err != nil {
		log.Error(err)
	}
}
