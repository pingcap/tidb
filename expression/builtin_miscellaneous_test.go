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
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestAnyValue(c *C) {
	defer testleak.AfterTest(c)()

	tbl := []struct {
		arg interface{}
		ret interface{}
	}{
		{nil, nil},
		{1234, 1234},
		{-0x99, -0x99},
		{3.1415926, 3.1415926},
		{"Hello, World", "Hello, World"},
	}
	for _, t := range tbl {
		fc := funcs[ast.AnyValue]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.arg)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestIsIPv6(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"2001:250:207:0:0:eef2::1", 1},
		{"2001:0250:0207:0001:0000:0000:0000:ff02", 1},
		{"2001:250:207::eff2::1，", 0},
		{"192.168.1.1", 0},
	}
	fc := funcs[ast.IsIPv6]
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
