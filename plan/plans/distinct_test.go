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
	"reflect"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testRowData struct {
	id   int64
	data []interface{}
}

type testTablePlan struct {
	rows   []*testRowData
	fields []string
}

var distinctTestData = []*testRowData{
	{1, []interface{}{10, "hello"}},
	{2, []interface{}{10, "hello"}},
	{3, []interface{}{10, "hello"}},
	{4, []interface{}{40, "hello"}},
	{6, []interface{}{60, "hello"}},
}

func (p *testTablePlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	for _, d := range p.rows {
		if more, err := f(d.id, d.data); !more || err != nil {
			return err
		}
	}
	return nil
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

func (p *testTablePlan) Next(ctx context.Context) (data []interface{}, rowKeys []*plan.RowKeyEntry, err error) {
	return
}

type testDistinctSuit struct{}

var _ = Suite(&testDistinctSuit{})

func (t *testDistinctSuit) TestDistinct(c *C) {
	tblPlan := &testTablePlan{distinctTestData, []string{"id", "name"}}

	p := DistinctDefaultPlan{
		SelectList: &SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
		},
		Src: tblPlan,
	}

	r := map[int][]interface{}{}
	err := p.Do(nil, func(id interface{}, data []interface{}) (bool, error) {
		r[data[0].(int)] = data
		return true, nil
	})
	c.Assert(err, IsNil)

	expected := map[int][]interface{}{
		10: {10, "hello"},
		40: {40, "hello"},
		60: {60, "hello"},
	}

	c.Assert(reflect.DeepEqual(r, expected), Equals, true)
}
