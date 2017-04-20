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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"math"
)

var bitCountCases = []struct {
	origin interface{}
	count  interface{}
}{
	{int64(8), int64(1)},
	{int64(29), int64(4)},
	{int64(0), int64(0)},
	{int64(-1), int64(64)},
	{int64(-11), int64(62)},
	{int64(-1000), int64(56)},
	{float64(1.1), int64(1)},
	{float64(3.1), int64(2)},
	{float64(-1.1), int64(64)},
	{float64(-3.1), int64(63)},
	{uint64(math.MaxUint64), int64(64)},
	{string("xxx"), int64(0)},
	{nil, nil},
}

func (s *testEvaluatorSuite) TestBitCount(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.BitCount]
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		count, err := f.eval(nil)
		c.Assert(err, IsNil)
		if count.IsNull() {
			c.Assert(test.count, IsNil)
			continue
		}
		sc := new(variable.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.count)
	}
}
