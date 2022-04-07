// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
)

func TestMergePartialResult4Varsamp(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarSamp, mysql.TypeDouble, 5, 2.5, 1, 1.9821428571428572),
	}
	for _, test := range tests {
		testMergePartialResult(t, test)
	}
}

func TestVarsamp(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarSamp, mysql.TypeDouble, 5, nil, 2.5),
	}
	for _, test := range tests {
		testAggFunc(t, test)
	}
}
