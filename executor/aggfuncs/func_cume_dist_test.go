// Copyright 2020 PingCAP, Inc.
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
)

func TestMemCumeDist(t *testing.T) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 1, 1,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 2, 0,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 4, 1,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
	}
	for _, test := range tests {
		testWindowAggMemFunc(t, test)
	}
}
