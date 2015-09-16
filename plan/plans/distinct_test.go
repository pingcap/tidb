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
	"reflect"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var distinctTestData = []*testRowData{
	{1, []interface{}{10, "hello"}},
	{2, []interface{}{10, "hello"}},
	{3, []interface{}{10, "hello"}},
	{4, []interface{}{40, "hello"}},
	{6, []interface{}{60, "hello"}},
}

type testDistinctSuit struct{}

var _ = Suite(&testDistinctSuit{})

func (t *testDistinctSuit) TestDistinct(c *C) {
	tblPlan := &testTablePlan{distinctTestData, []string{"id", "name"}, 0}

	p := plans.DistinctDefaultPlan{
		SelectList: &plans.SelectList{
			HiddenFieldOffset: len(tblPlan.GetFields()),
		},
		Src: tblPlan,
	}
	rset := rsets.Recordset{
		Plan: &p,
		Ctx:  mock.NewContext(),
	}
	r := map[int][]interface{}{}
	err := rset.Do(func(data []interface{}) (bool, error) {
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
