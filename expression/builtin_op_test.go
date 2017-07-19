// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testEvaluatorSuite) TestUnary(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		args     interface{}
		expected interface{}
		overflow bool
		getErr   bool
	}{
		{uint64(9223372036854775809), "-9223372036854775809", true, false},
		{uint64(9223372036854775810), "-9223372036854775810", true, false},
		{uint64(9223372036854775808), "-9223372036854775808", false, false},
		{int64(math.MinInt64), "9223372036854775808", false, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryMinus, primitiveValsToConstants([]interface{}{t.args})...)

		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if !t.overflow {
				strd, _ := d.ToString()
				c.Assert(strd, Equals, t.expected)
			} else {
				c.Assert(d.GetMysqlDecimal().String(), Equals, t.expected)
			}
		}
	}
}
