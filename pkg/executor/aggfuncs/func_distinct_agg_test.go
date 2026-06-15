// Copyright 2025 PingCAP, Inc.
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
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func TestParallelDistinctCount(t *testing.T) {
	dataTypes := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeNewDecimal), types.NewFieldType(mysql.TypeVarString), types.NewFieldType(mysql.TypeDuration)}
	testParallelDistinctAggCases(t, ast.AggFuncCount, dataTypes, 10000, false)

	// Test countPartialWithDistinct and countOriginalWithDistinct
	dataTypes = []*types.FieldType{types.NewFieldType(mysql.TypeVarString), types.NewFieldType(mysql.TypeVarString)}
	testParallelDistinctAggCases(t, ast.AggFuncCount, dataTypes, 100, true)
}

func TestParallelDistinctSum(t *testing.T) {
	dataTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeNewDecimal)}
	testParallelDistinctAggCases(t, ast.AggFuncSum, dataTypes, 10000, false)
}

func TestParallelDistinctAvg(t *testing.T) {
	dataTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeNewDecimal)}
	testParallelDistinctAggCases(t, ast.AggFuncAvg, dataTypes, 10000, false)
}

func TestParallelDistinctVarAndStddev(t *testing.T) {
	for _, funcName := range []string{ast.AggFuncVarPop, ast.AggFuncVarSamp, ast.AggFuncStddevPop, ast.AggFuncStddevSamp} {
		t.Run(funcName, func(t *testing.T) {
			dataTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble)}
			testParallelDistinctAggCases(t, funcName, dataTypes, 10000, false)
		})
	}
}

func TestParallelDistinctGroupConcat(t *testing.T) {
	dataTypes := []*types.FieldType{types.NewFieldType(mysql.TypeVarString), types.NewFieldType(mysql.TypeVarString)}
	testParallelDistinctAggCases(t, ast.AggFuncGroupConcat, dataTypes, 100, true)
}

func testParallelDistinctAggCases(t *testing.T, funcName string, dataTypes []*types.FieldType, maxRows int, multiArgs bool) {
	t.Helper()

	for range 10 {
		var testCase *parallelDistinctAggTestCase
		randNum := rand.Intn(100)
		inputTypes := dataTypes
		if !multiArgs {
			inputTypes = []*types.FieldType{dataTypes[rand.Intn(len(dataTypes))]}
		}
		if randNum < 10 {
			// Empty input
			testCase = newParallelDistinctAggTestCase(funcName, inputTypes, 0, 0, false, false)
		} else if randNum < 20 {
			// Some input are null
			rowNum := rand.Intn(maxRows) + 1
			testCase = newParallelDistinctAggTestCase(funcName, inputTypes, rowNum, rand.Intn(rowNum)+1, true, false)
		} else if randNum < 30 {
			// All input are null
			rowNum := rand.Intn(maxRows) + 1
			testCase = newParallelDistinctAggTestCase(funcName, inputTypes, rowNum, rand.Intn(rowNum)+1, true, true)
		} else {
			// All input are not null
			rowNum := rand.Intn(maxRows) + 1
			testCase = newParallelDistinctAggTestCase(funcName, inputTypes, rowNum, rand.Intn(rowNum)+1, false, false)
		}

		testParallelDistinctAggFunc(t, *testCase, multiArgs)
	}
}
