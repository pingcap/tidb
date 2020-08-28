// Copyright 2018 PingCAP, Inc.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
)

func (s *testSuite) TestMergePartialResult4Avg(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5, 2.0, 3.0, 2.375),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, 5, 2.0, 3.0, 2.375),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestAvg(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5, nil, 2.0),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, 5, nil, 2.0),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemAvg(c *C) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDecimalSize, defaultUpdateMemDeltaGens, false, nil, 2.0),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDistinctDecimalSize, distinctUpdateMemDeltaGens, true, nil, 2.0),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgFloat64Size, defaultUpdateMemDeltaGens, false, nil, 2.0),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgDistinctFloat64Size, distinctUpdateMemDeltaGens, true, nil, 2.0),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

func BenchmarkAvg(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

	rowNum := 50000
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, rowNum, nil, 2.0),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, rowNum, nil, 2.0),
	}
	for _, test := range tests {
		s.benchmarkAggFunc(b, test)
	}
}
