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
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/format"
)

type testRowData struct {
	id   int64
	data []interface{}
}

type testTablePlan struct {
	rows   []*testRowData
	fields []string
	cursor int
}

func (p *testTablePlan) Explain(w format.Formatter) {}

func (p *testTablePlan) GetFields() []*field.ResultField {
	var ret []*field.ResultField
	for _, fn := range p.fields {
		ret = append(ret, &field.ResultField{
			Name: fn,
		})
	}
	return ret
}

func (p *testTablePlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return p, false, nil
}

func (p *testTablePlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if p.cursor == len(p.rows) {
		return
	}
	row = &plan.Row{
		Data: p.rows[p.cursor].data,
	}
	p.cursor++
	return
}

func (p *testTablePlan) Close() error {
	p.cursor = 0
	return nil
}

type testLimitSuit struct {
	data []*testRowData
	sess tidb.Session
}

var _ = Suite(&testLimitSuit{
	data: []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	},
})

func (t *testLimitSuit) SetUpSuite(c *C) {
	store, _ := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	t.sess, _ = tidb.CreateSession(store)
}

func (t *testLimitSuit) TestLimit(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}, 0}

	pln := &plans.LimitDefaultPlan{
		Count:  2,
		Src:    tblPlan,
		Fields: []*field.ResultField{},
	}
	rset := rsets.Recordset{
		Ctx:  t.sess.(context.Context),
		Plan: pln,
	}
	rset.Do(func(data []interface{}) (bool, error) {
		// TODO check result
		return true, nil
	})
}

func (t *testLimitSuit) TestOffset(c *C) {
	tblPlan := &testTablePlan{t.data, []string{"id", "name"}, 0}

	pln := &plans.OffsetDefaultPlan{
		Count:  2,
		Src:    tblPlan,
		Fields: []*field.ResultField{},
	}

	rset := rsets.Recordset{
		Ctx:  t.sess.(context.Context),
		Plan: pln,
	}
	rset.Do(func(data []interface{}) (bool, error) {
		// TODO check result
		return true, nil
	})

}
