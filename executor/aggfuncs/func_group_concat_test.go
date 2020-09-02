// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
)

func (s *testSuite) TestMergePartialResult4GroupConcat(c *C) {
	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, "0 1 2 3 4", "2 3 4", "0 1 2 3 4 2 3 4")
	s.testMergePartialResult(c, test)

	test2 := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, "0 1 2 3 4", "2 3 4", "0 1 2 3 4")
	s.testMergePartialResultWithDistinct(c, test2)
}

func (s *testSuite) TestGroupConcat(c *C) {
	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, nil, "0 1 2 3 4")
	s.testAggFunc(c, test)

	test2 := buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00")
	test2.orderBy = true
	s.testMultiArgsAggFunc(c, test2)

	defer variable.SetSessionSystemVar(s.ctx.GetSessionVars(), variable.GroupConcatMaxLen, types.NewStringDatum("1024"))
	// minimum GroupConcatMaxLen is 4
	for i := 4; i <= 7; i++ {
		variable.SetSessionSystemVar(s.ctx.GetSessionVars(), variable.GroupConcatMaxLen, types.NewStringDatum(fmt.Sprint(i)))
		test2 = buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00"[:i])
		test2.orderBy = true
		s.testMultiArgsAggFunc(c, test2)
	}
}

func BenchmarkGroupConcat(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

	rowNum := 50000
	tests := []aggTest{
		buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, rowNum, nil, ""),
	}
	for _, test := range tests {
		s.benchmarkAggFunc(b, test)
	}
}
