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

type testWhereSuit struct {
	data []*testRowData
}

var _ = Suite(&testWhereSuit{
	data: []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	},
})

func (t *testWhereSuit) TestWhere(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}, 0}
	pln := &plans.FilterDefaultPlan{
		Plan: tblPlan,
		Expr: &expression.BinaryOperation{
			Op: opcode.GE,
			L: &expression.Ident{
				CIStr:      model.NewCIStr("id"),
				ReferScope: expression.IdentReferFromTable,
				ReferIndex: 0,
			},
			R: expression.Value{
				Val: 30,
			},
		},
	}

	cnt := 0
	rset := rsets.Recordset{Plan: pln,
		Ctx: mock.NewContext()}
	rset.Do(func(data []interface{}) (bool, error) {
		cnt++
		return true, nil
	})
	c.Assert(cnt, Equals, 2)
}
