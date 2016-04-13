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
	"github.com/pingcap/tidb/mysql"
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
		v := mysql.NewDecimalFromFloat(input.Input)
		b := EncodeDecimal([]byte{}, v)
		_, d, err := DecodeDecimal(b)
		c.Assert(err, IsNil)
		c.Assert(v.Equals(d), IsTrue)
	}
}
