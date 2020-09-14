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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func (s *testSuite) TestPercentile(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeLonglong, 5, nil, 2),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeFloat, 5, nil, 2.0),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeDouble, 5, nil, 2.0),
		buildAggTester(ast.AggFuncApproxPercentile, mysql.TypeNewDecimal, 5, nil, types.NewDecFromFloatForTest(2.0)),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
