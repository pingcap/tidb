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
	"github.com/pingcap/tidb/executor/aggfuncs"
)

func (s *testSuite) TestMemValue(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeLonglong, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4IntSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeFloat, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float32Size, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDouble, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float64Size, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeNewDecimal, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DecimalSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeString, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4StringSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDate, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4TimeSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDuration, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DurationSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeJSON, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4JSONSize, defaultUpdateMemDeltaGens, false),

		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeLonglong, 1, 0, 2,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4IntSize, defaultUpdateMemDeltaGens, false),

		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 2, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, defaultUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong,
			5, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, defaultUpdateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testWindowMemFunc(c, test)
	}
}
