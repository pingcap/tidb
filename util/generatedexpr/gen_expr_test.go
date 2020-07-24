// Copyright 2017 PingCAP, Inc.
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

package generatedexpr

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
)

var _ = Suite(&testGenExprSuite{})

type testGenExprSuite struct{}

func (s *testGenExprSuite) TestParseExpression(c *C) {
	tests := []struct {
		input   string
		output  string
		success bool
	}{
		{"json_extract(a, '$.a')", "json_extract", true},
	}
	for _, tt := range tests {
		node, err := ParseExpression(tt.input)
		if tt.success {
			fc := node.(*ast.FuncCallExpr)
			c.Assert(fc.FnName.L, Equals, tt.output)
		} else {
			c.Assert(err, NotNil)
		}
	}
}
