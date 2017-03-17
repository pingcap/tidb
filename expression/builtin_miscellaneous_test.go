// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestIsIPv4(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"192.168.1.1", 1},
		{"255.255.255.255", 1},
		{"10.t.255.255", 0},
		{"10.1.2.3.4", 0},
	}
	fc := funcs[ast.IsIPv4]
	for _, test := range tests {
		ip := types.NewStringDatum(test.ip)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{ip}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}
