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

package codec

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testDecimalSuite{})

type testDecimalSuite struct {
}

func (s *testDecimalSuite) TestDecimalCodec(c *C) {
	defer testleak.AfterTest(c)()
	inputs := []struct {
		Input float64
	}{
		{float64(123400)},
		{float64(1234)},
		{float64(12.34)},
		{float64(0.1234)},
		{float64(0.01234)},
		{float64(-0.1234)},
		{float64(-0.01234)},
		{float64(12.3400)},
		{float64(-12.34)},
		{float64(0.00000)},
		{float64(0)},
		{float64(-0.0)},
		{float64(-0.000)},
	}

	for _, input := range inputs {
		v := types.NewDecFromFloatForTest(input.Input)
		datum := types.NewDatum(v)
		b := EncodeDecimal([]byte{}, datum.GetMysqlDecimal(), datum.Length(), datum.Frac())
		_, d, err := DecodeDecimal(b)
		c.Assert(err, IsNil)
		c.Assert(v.Compare(d.GetMysqlDecimal()), Equals, 0)
	}
}

func (s *testDecimalSuite) TestFrac(c *C) {
	defer testleak.AfterTest(c)()
	inputs := []struct {
		Input *types.MyDecimal
	}{
		{types.NewDecFromInt(3)},
		{types.NewDecFromFloatForTest(0.03)},
	}
	for _, v := range inputs {
		testFrac(c, v.Input)
	}
}

func testFrac(c *C, v *types.MyDecimal) {
	var d1 types.Datum
	d1.SetMysqlDecimal(v)
	b := EncodeDecimal([]byte{}, d1.GetMysqlDecimal(), d1.Length(), d1.Frac())
	_, d2, err := DecodeDecimal(b)
	c.Assert(err, IsNil)
	sc := new(stmtctx.StatementContext)
	cmp, err := d1.CompareDatum(sc, &d2)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	c.Assert(d1.GetMysqlDecimal().String(), Equals, d2.GetMysqlDecimal().String())
}
