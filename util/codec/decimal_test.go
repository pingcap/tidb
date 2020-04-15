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
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/testleak"
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

		b, err := EncodeDecimal([]byte{}, datum.GetMysqlDecimal(), datum.Length(), datum.Frac())
		c.Assert(err, IsNil)
		_, d, prec, frac, err := DecodeDecimal(b)
		if datum.Length() != 0 {
			c.Assert(prec, Equals, datum.Length())
			c.Assert(frac, Equals, datum.Frac())
		} else {
			prec1, frac1 := datum.GetMysqlDecimal().PrecisionAndFrac()
			c.Assert(prec, Equals, prec1)
			c.Assert(frac, Equals, frac1)
		}
		c.Assert(err, IsNil)
		c.Assert(v.Compare(d), Equals, 0)
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

	b, err := EncodeDecimal([]byte{}, d1.GetMysqlDecimal(), d1.Length(), d1.Frac())
	c.Assert(err, IsNil)
	_, dec, _, _, err := DecodeDecimal(b)
	c.Assert(err, IsNil)
	c.Assert(dec.String(), Equals, v.String())
}
