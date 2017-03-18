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
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestIsIPv6(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"::ffff:127.1.2.3", 1},
		{"::ffff:7f01:0203", 1},
		{":10.3.2.2", 0},
		{"NotAIP", 0},
	}
	fc := funcs[ast.IsIPv4Mapped]
	for _, test := range tests {
		ip := types.NewStringDatum(test.ip)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{ip}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}
	// test NULL input for is_ipv6
	var argNull types.Datum
	f, _ := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(0))
}
