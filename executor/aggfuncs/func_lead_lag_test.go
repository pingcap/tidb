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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/types"
)

func (s *testSuite) TestLeadLag(c *C) {
	zero := expression.Zero
	one := expression.One
	two := &expression.Constant{
		Value:   types.NewDatum(2),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	three := &expression.Constant{
		Value:   types.NewDatum(3),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	million := &expression.Constant{
		Value:   types.NewDatum(1000000),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	defaultArg := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}

	numRows := 3
	tests := []windowTest{
		// lag(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, nil, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, nil, nil, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, nil, nil, nil),
		// lag(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero, million}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one, million}, 0, numRows, 1000000, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two, million}, 0, numRows, 1000000, 1000000, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three, million}, 0, numRows, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million, million}, 0, numRows, 1000000, 1000000, 1000000),
		// lag(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one, defaultArg}, 0, numRows, 0, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two, defaultArg}, 0, numRows, 0, 1, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million, defaultArg}, 0, numRows, 0, 1, 2),

		// lead(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, 1, 2, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, 2, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, nil, nil, nil),
		// lead(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero, million}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one, million}, 0, numRows, 1, 2, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two, million}, 0, numRows, 2, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three, million}, 0, numRows, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million, million}, 0, numRows, 1000000, 1000000, 1000000),
		// lead(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one, defaultArg}, 0, numRows, 1, 2, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two, defaultArg}, 0, numRows, 2, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million, defaultArg}, 0, numRows, 0, 1, 2),
	}
	for _, test := range tests {
		s.testWindowFunc(c, test)
	}
}
