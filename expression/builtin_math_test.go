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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestAbs(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{int64(-1), int64(1)},
		{float64(3.14), float64(3.14)},
		{float64(-3.14), float64(3.14)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinAbs(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCeil(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{float64(1.23), float64(2)},
		{float64(-1.23), float64(-1)},
		{"1.23", float64(2)},
		{"-1.23", float64(-1)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinCeil(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}
}

func (s *testEvaluatorSuite) TestLog(c *C) {
	defer testleak.AfterTest(c)()

	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{int64(2)}, float64(0.6931471805599453)},

		{[]interface{}{int64(2), int64(65536)}, float64(16)},
		{[]interface{}{int64(10), int64(100)}, float64(2)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinLog(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}

	nullTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{int64(-2)}},
		{[]interface{}{int64(1), int64(100)}},
	}

	nullDtbl := tblToDtbl(nullTbl)

	for _, t := range nullDtbl {
		v, err := builtinLog(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v.Kind(), Equals, types.KindNull)
	}
}

func (s *testEvaluatorSuite) TestRand(c *C) {
	defer testleak.AfterTest(c)()
	v, err := builtinRand(make([]types.Datum, 0), s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Less, float64(1))
	c.Assert(v.GetFloat64(), GreaterEqual, float64(0))
}

func (s *testEvaluatorSuite) TestPow(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret float64
	}{
		{[]interface{}{1, 3}, 1},
		{[]interface{}{2, 2}, 4},
		{[]interface{}{4, 0.5}, 2},
		{[]interface{}{4, -2}, 0.0625},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinPow(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}

	errTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{"test", "test"}},
		{[]interface{}{nil, nil}},
		{[]interface{}{1, "test"}},
		{[]interface{}{1, nil}},
	}

	errDtbl := tblToDtbl(errTbl)
	for _, t := range errDtbl {
		_, err := builtinPow(t["Arg"], s.ctx)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestRound(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret float64
	}{
		{[]interface{}{-1.23}, -1},
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{-1.58}, -2},
		{[]interface{}{1.58}, 2},
		{[]interface{}{1.298, 1}, 1.3},
		{[]interface{}{1.298}, 1},
		{[]interface{}{1.298, 0}, 1},
		{[]interface{}{23.298, -1}, 20},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinRound(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCRC32(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret uint64
	}{
		{[]interface{}{"mysql"}, 2501908538},
		{[]interface{}{"MySQL"}, 3259397556},
		{[]interface{}{"hello"}, 907060870},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		v, err := builtinCRC32(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}
