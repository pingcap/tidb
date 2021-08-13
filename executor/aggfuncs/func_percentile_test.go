// Copyright 2020 PingCAP, Inc.
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

package aggfuncs_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/types"
)

type testSlice []int

func (a testSlice) Len() int           { return len(a) }
func (a testSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a testSlice) Less(i, j int) bool { return a[i] < a[j] }

func (s *testSuite) TestPercentile(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeLonglong, 5, nil, 2),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeFloat, 5, nil, 2.0),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeDouble, 5, nil, 2.0),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeNewDecimal, 5, nil, types.NewDecFromFloatForTest(2.0)),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeDate, 5, nil, types.TimeFromDays(367)),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(2)}),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}

	data := testSlice{}
	for i := 1; i <= 28; i++ {
		data = append(data, i)
	}
	for i := 0; i < 10; i++ {
		index := aggfuncs.PercentileForTesting(data, 100)
		c.Assert(28, Equals, data[index])
	}
}
