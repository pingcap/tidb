// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"strings"
)

func (s *testEvaluatorSuite) TestUUID(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.UUID]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums()), s.ctx)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
	parts := strings.Split(r.GetString(), "-")
	c.Assert(len(parts), Equals, 5)
	for i, p := range parts {
		switch i {
		case 0:
			c.Assert(len(p), Equals, 8)
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			c.Assert(len(p), Equals, 4)
		case 4:
			c.Assert(len(p), Equals, 12)
		}
	}
}
