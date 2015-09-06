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
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/model"
)

type testOrderBySuit struct {
	data []*testRowData
}

var _ = Suite(&testOrderBySuit{
	[]*testRowData{
		&testRowData{1, []interface{}{10, "10"}},
		&testRowData{2, []interface{}{10, "20"}},
		&testRowData{3, []interface{}{10, "30"}},
		&testRowData{4, []interface{}{40, "40"}},
		&testRowData{6, []interface{}{60, "60"}},
	},
})

func (t *testOrderBySuit) TestOrderBy(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}}

	pln := &OrderByDefaultPlan{
		SelectList: &SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
			ResultFields:      tblPlan.GetFields(),
		},
		Src: tblPlan,
		By: []expression.Expression{
			&expressions.Ident{
				CIStr: model.NewCIStr("id"),
			},
		},
		Ascs: []bool{false},
	}

	prev := 10000
	err := pln.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
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
