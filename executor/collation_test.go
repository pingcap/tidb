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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
)

var _ = SerialSuites(&testCollationSuite{})

type testCollationSuite struct {
}

func (s *testCollationSuite) TestVecGroupChecker(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tp := &types.FieldType{Tp: mysql.TypeVarchar}
	col0 := &expression.Column{
		RetType: tp,
		Index:   0,
	}
	ctx := mock.NewContext()
	groupChecker := newVecGroupChecker(ctx, []expression.Expression{col0})

	chk := chunk.New([]*types.FieldType{tp}, 6, 6)
	chk.Reset()
	chk.Column(0).AppendString("aaa")
	chk.Column(0).AppendString("AAA")
	chk.Column(0).AppendString("😜")
	chk.Column(0).AppendString("😃")
	chk.Column(0).AppendString("À")
	chk.Column(0).AppendString("A")

	tp.Collate = "bin"
	groupChecker.reset()
	_, err := groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 6; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i)
		c.Assert(e, Equals, i+1)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	tp.Collate = "utf8_general_ci"
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i*2)
		c.Assert(e, Equals, i*2+2)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	tp.Collate = "utf8_unicode_ci"
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i*2)
		c.Assert(e, Equals, i*2+2)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	// test padding
	tp.Collate = "utf8_bin"
	tp.Flen = 6
	chk.Reset()
	chk.Column(0).AppendString("a")
	chk.Column(0).AppendString("a  ")
	chk.Column(0).AppendString("a    ")
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	b, e := groupChecker.getNextGroup()
	c.Assert(b, Equals, 0)
	c.Assert(e, Equals, 3)
	c.Assert(groupChecker.isExhausted(), IsTrue)
}
