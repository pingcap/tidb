// Copyright 2016 PingCAP, Inc.
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

package evaluator

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestIsNullOpFactory(c *C) {
	defer testleak.AfterTest(c)()

	nullOp := isNullOpFactory(opcode.Null)
	v, err := nullOp(types.MakeDatums(1), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))
	v, err = nullOp(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))

	notNullOp := isNullOpFactory(opcode.NotNull)
	v, err = notNullOp(types.MakeDatums(1), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
	v, err = notNullOp(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))
}
