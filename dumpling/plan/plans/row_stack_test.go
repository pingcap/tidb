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
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
)

type testOuterQuerySuite struct{}

var _ = Suite(&testOuterQuerySuite{})

func (s *testOuterQuerySuite) TestRowStackFromPlan(c *C) {
	var data = []*testRowData{
		{1, []interface{}{10, "hello"}},
	}

	pln := &testTablePlan{data, []string{"id", "name"}, 0}

	p := &plans.RowStackFromPlan{
		Src: pln,
	}

	fields := p.GetFields()
	c.Assert(fields, HasLen, 2)

	rset := rsets.Recordset{
		Plan: p,
		Ctx:  mock.NewContext()}
	_, err := rset.Rows(-1, 0)
	c.Assert(err, IsNil)
}
