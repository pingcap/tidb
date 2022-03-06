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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"testing"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
)

func TestMergePartialResult4Avg(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5, 2.0, 3.0, 2.375),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, 5, 2.0, 3.0, 2.375),
	}
	for _, test := range tests {
		testMergePartialResult(t, test)
	}
}

func TestAvg(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5, nil, 2.0),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, 5, nil, 2.0),
	}

	for _, test := range tests {
		testAggFunc(t, test)
	}
}

func TestMemAvg(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDecimalSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDistinctDecimalSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncAvg, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgDistinctFloat64Size+set.DefFloat64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
	}
	for _, test := range tests {
		testAggMemFunc(t, test)
	}
}

func BenchmarkAvg(b *testing.B) {
	ctx := mock.NewContext()

	rowNum := 50000
	tests := []aggTest{
		buildAggTester(ast.AggFuncAvg, mysql.TypeNewDecimal, rowNum, nil, 2.0),
		buildAggTester(ast.AggFuncAvg, mysql.TypeDouble, rowNum, nil, 2.0),
	}
	for _, test := range tests {
		benchmarkAggFunc(b, ctx, test)
	}
}
