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
)

func (s *testSuite) TestMergePartialResult4GroupConcat(c *C) {
	test := buildAggMergeTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, "0 1 2 3 4", "2 3 4", "0 1 2 3 4 2 3 4")
	s.testMergePartialResult(c, test)
}
