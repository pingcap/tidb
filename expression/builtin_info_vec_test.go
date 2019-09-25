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

package expression

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
)

var vecBuiltinInfoCases = map[string][]vecExprBenchCase{
	ast.Decode: {
	},
	ast.TiDBVersion: {
	},
	ast.Date: {
	},
	ast.Timestamp: {
	},
	ast.CurrentUser: {
	},
	ast.FoundRows: {
	},
	ast.Database: {
	},
	ast.Format: {
	},
	ast.User: {
	},
	ast.TiDBDecodeKey: {
	},
	ast.RowCount: {
	},
	ast.CurrentRole: {
	},
	ast.Time: {
	},
	ast.TiDBIsDDLOwner: {
	},
	ast.ConnectionID: {
	},}

func (s *testEvaluatorSuite) TestVectorizedBuiltinInfoFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinInfoCases)
}

func BenchmarkVectorizedBuiltinInfoFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinInfoCases)
}
