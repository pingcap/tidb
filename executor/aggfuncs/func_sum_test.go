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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
)

func TestMergePartialResult4Sum(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncSum, mysql.TypeNewDecimal, 5, types.NewDecFromInt(10), types.NewDecFromInt(9), types.NewDecFromInt(19)),
		buildAggTester(ast.AggFuncSum, mysql.TypeDouble, 5, 10.0, 9.0, 19.0),
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.funcName, i), func(t *testing.T) {
			testMergePartialResult(t, test)
		})
	}
}

func TestSum(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncSum, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(10)),
		buildAggTester(ast.AggFuncSum, mysql.TypeDouble, 5, nil, 10.0),
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.funcName, i), func(t *testing.T) {
			testAggFunc(t, test)
		})
	}
}

func TestMemSum(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncSum, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4SumFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncSum, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4SumDecimalSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncSum, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4SumDistinctFloat64Size+set.DefFloat64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncSum, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4SumDistinctDecimalSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.aggTest.funcName, i), func(t *testing.T) {
			testAggMemFunc(t, test)
		})
	}
}
