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
	"math"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestUUID(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.UUID]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums()), s.ctx)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
	parts := strings.Split(r.GetString(), "-")
	c.Assert(len(parts), Equals, 5)
	for i, p := range parts {
		switch i {
		case 0:
			c.Assert(len(p), Equals, 8)
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			c.Assert(len(p), Equals, 4)
		case 4:
			c.Assert(len(p), Equals, 12)
		}
	}
}

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

func (s *testEvaluatorSuite) TestInetNtoa(c *C) {
	tests := []struct {
		ip     int
		expect interface{}
	}{
		{167773449, "10.0.5.9"},
		{2063728641, "123.2.0.1"},
		{0, "0.0.0.0"},
		{545460846593, nil},
		{-1, nil},
		{math.MaxUint32, "255.255.255.255"},
	}
	fc := funcs[ast.InetNtoa]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
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
