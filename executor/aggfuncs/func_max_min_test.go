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
	tests := []aggMergeTest{
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeLonglong, 5, 4, 4, 4),
		buildAggMergeTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeFloat, 5, 4.0, 4.0, 4.0),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeDouble, 5, 4.0, 4.0, 4.0),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, types.NewDecFromInt(4), types.NewDecFromInt(4), types.NewDecFromInt(4)),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeString, 5, "4", "4", "4"),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeDate, 5, types.TimeFromDays(4), types.TimeFromDays(4), types.TimeFromDays(4)),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}),
		buildAggMergeTester(ast.AggFuncMax, mysql.TypeJSON, 5, json.CreateBinary(int64(4)), json.CreateBinary(int64(4)), json.CreateBinary(int64(4))),

		buildAggMergeTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggMergeTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeDate, 5, types.TimeFromDays(0), types.TimeFromDays(2), types.TimeFromDays(0)),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggMergeTester(ast.AggFuncMin, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}
