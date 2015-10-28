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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

type testUnionSuit struct {
	data  []*testRowData
	data2 []*testRowData
}

var _ = Suite(&testUnionSuit{
	data: []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	},
	data2: []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{70, "60"}},
	},
})

func (t *testUnionSuit) TestUnion(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}, 0}
	tblPlan2 := &testTablePlan{t.data2, []string{"id", "name"}, 0}
	cols := []*column.Col{
		{
			ColumnInfo: model.ColumnInfo{
				ID:           0,
				Name:         model.NewCIStr("id"),
				Offset:       0,
				DefaultValue: 0,
				FieldType:    *types.NewFieldType(mysql.TypeLonglong),
			},
		},
		{
			ColumnInfo: model.ColumnInfo{
				ID:           1,
				Name:         model.NewCIStr("name"),
				Offset:       1,
				DefaultValue: nil,
				FieldType:    *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	}

	// Set Flen explicitly here, because following ColToResultField will change Flen to zero.
	// TODO: remove this if ColToResultField update.
	cols[1].FieldType.Flen = 100

	pln := &plans.UnionPlan{
		Srcs: []plan.Plan{
			tblPlan,
			tblPlan2,
		},
		Distincts: []bool{true},
		RFields: []*field.ResultField{
			field.ColToResultField(cols[0], "t"),
			field.ColToResultField(cols[1], "t"),
		},
	}
	rset := rsets.Recordset{
		Plan: pln,
		Ctx:  mock.NewContext()}
	cnt := 0
	rset.Do(func(data []interface{}) (bool, error) {
		cnt++
		return true, nil
	})
	c.Assert(cnt, Equals, 6)
}
