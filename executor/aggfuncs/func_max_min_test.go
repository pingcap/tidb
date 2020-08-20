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
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testSuite) TestMergePartialResult4MaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, 4, 4, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, types.NewDecFromInt(4), types.NewDecFromInt(4), types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, "4", "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, types.TimeFromDays(369), types.TimeFromDays(369), types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, json.CreateBinary(int64(4)), json.CreateBinary(int64(4)), json.CreateBinary(int64(4))),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, nil, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, nil, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, nil, "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, nil, types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(4))),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, nil, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, nil, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, nil, "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, nil, types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestUnBoundedMaxMinBuilder(c *C) {
	one := expression.NewOne()

	desc, err := aggregation.NewWindowAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{one}, false, true)
	c.Assert(err, IsNil)
	aggFunc := aggfuncs.BuildWindowFunctions(s.ctx, desc, 0, []*expression.Column{{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}})
	_, ok := aggFunc.(aggfuncs.SlidingWindowAggFunc)
	c.Assert(ok, IsFalse)

	desc, err = aggregation.NewWindowAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{one}, false, false)
	c.Assert(err, IsNil)
	aggFunc = aggfuncs.BuildWindowFunctions(s.ctx, desc, 0, []*expression.Column{{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}})
	_, ok = aggFunc.(aggfuncs.SlidingWindowAggFunc)
	c.Assert(ok, IsTrue)
}
