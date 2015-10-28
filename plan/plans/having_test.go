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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
)

type testHavingPlan struct{}

var _ = Suite(&testHavingPlan{})

func (t *testHavingPlan) TestHaving(c *C) {
	tblPlan := &testTablePlan{groupByTestData, []string{"id", "name"}, 0}
	havingPlan := &plans.HavingPlan{
		SelectList: &plans.SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
			ResultFields:      tblPlan.GetFields(),
		},
		Src: tblPlan,
		Expr: &expression.BinaryOperation{
			Op: opcode.GE,
			L: &expression.Ident{
				CIStr:      model.NewCIStr("id"),
				ReferScope: expression.IdentReferSelectList,
				ReferIndex: 0,
			},
			R: &expression.Value{
				Val: 20,
			},
		},
	}

	// having's behavior just like where
	cnt := 0
	rset := rsets.Recordset{
		Plan: havingPlan,
		Ctx:  mock.NewContext(),
	}
	rset.Do(func(data []interface{}) (bool, error) {
		cnt++
		return true, nil
	})
	c.Assert(cnt, Equals, 2)
}
