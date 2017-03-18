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

func (s *testEvaluatorSuite) TestInetAton(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"10.0.5.9", 167773449},
		{"::ffff:10.0.5.9", 167773449},
		{"NotAIP", nil},
		{"2001:da8::1", nil},
	}
	fc := funcs[ast.InetAton]
	for _, test := range tests {
		ip := types.NewStringDatum(test.ip)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{ip}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}
