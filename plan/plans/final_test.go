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
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
)

type testFinalPlan struct{}

var _ = Suite(&testFinalPlan{})

var finalTestData = []*testRowData{
	{1, []interface{}{10, "hello", true}},
	{2, []interface{}{10, "hello", true}},
	{3, []interface{}{10, "hello", true}},
	{4, []interface{}{40, "hello", true}},
	{6, []interface{}{60, "hello", false}},
}

func (t *testFinalPlan) TestFinalPlan(c *C) {
	col1 := &column.Col{
		ColumnInfo: model.ColumnInfo{
			ID:           0,
			Name:         model.NewCIStr("id"),
			Offset:       0,
			DefaultValue: 0,
		},
	}

	col2 := &column.Col{
		ColumnInfo: model.ColumnInfo{
			ID:           1,
			Name:         model.NewCIStr("name"),
			Offset:       1,
			DefaultValue: nil,
		},
	}

	col3 := &column.Col{
		ColumnInfo: model.ColumnInfo{
			ID:           2,
			Name:         model.NewCIStr("ok"),
			Offset:       2,
			DefaultValue: false,
		},
	}

	tblPlan := &testTablePlan{finalTestData, []string{"id", "name", "ok"}, 0}

	p := &plans.SelectFinalPlan{
		SelectList: &plans.SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
			ResultFields: []*field.ResultField{
				field.ColToResultField(col1, "t"),
				field.ColToResultField(col2, "t"),
				field.ColToResultField(col3, "t"),
			},
		},
		Src: tblPlan,
	}

	for _, rf := range p.ResultFields {
		c.Assert(rf.Col.Flag, Equals, uint(0))
		c.Assert(rf.Col.Tp, Equals, byte(0))
		c.Assert(rf.Col.Charset, Equals, "")
	}

	rset := rsets.Recordset{
		Plan: p,
		Ctx:  mock.NewContext()}
	rset.Do(func(in []interface{}) (bool, error) {
		return true, nil
	})

	for _, rf := range p.ResultFields {
		if rf.Name == "id" {
			c.Assert(rf.Col.Tp, Equals, mysql.TypeLonglong)
			c.Assert(rf.Col.Charset, Equals, charset.CharsetBin)
		}
		if rf.Name == "name" {
			c.Assert(rf.Col.Tp, Equals, mysql.TypeVarchar)
		}
		// TODO add more type tests
	}
}
