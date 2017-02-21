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

package plan

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
)

var _ = Suite(&testPlanBuilderSuite{})

func (s *testPlanBuilderSuite) SetUpSuite(c *C) {
}

type testPlanBuilderSuite struct {
}

func (s *testPlanBuilderSuite) TestShowDatabases(c *C) {
	pb := &planBuilder{
		allocator: new(idAllocator),
	}
	node := &ast.ShowStmt{
		Tp: ast.ShowDatabases,
	}
	p := pb.build(node)
	schema := p.Schema()
	c.Assert(schema.Columns, HasLen, 1)
	c.Assert(schema.Columns[0].RetType.Flen, Equals, 256)
}
