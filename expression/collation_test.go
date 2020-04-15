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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/collate"
	"github.com/pingcap/tidb/v4/util/mock"
)

var _ = SerialSuites(&testCollationSuites{})

type testCollationSuites struct{}

func (s *testCollationSuites) TestCompareString(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	c.Assert(types.CompareString("a", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("À", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("😜", "😃", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("a ", "a  ", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("a", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("À", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("😜", "😃", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("a ", "a  ", "binary"), Not(Equals), 0)

	ctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeVarString)
	col1 := &Column{
		RetType: ft,
		Index:   0,
	}
	col2 := &Column{
		RetType: ft,
		Index:   1,
	}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)
	chk.Column(0).AppendString("a")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("À")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("😜")
	chk.Column(1).AppendString("😃")
	chk.Column(0).AppendString("a ")
	chk.Column(1).AppendString("a  ")
	for i := 0; i < 4; i++ {
		v, isNull, err := CompareStringWithCollationInfo(ctx, col1, col2, chk.GetRow(0), chk.GetRow(0), "utf8_general_ci")
		c.Assert(err, IsNil)
		c.Assert(isNull, IsFalse)
		c.Assert(v, Equals, int64(0))
	}
}

func (s *testCollationSuites) TestDeriveCollationFromExprs(c *C) {
	tInt := types.NewFieldType(mysql.TypeLonglong)
	ctx := mock.NewContext()

	// no string column
	chs, coll, flen := DeriveCollationFromExprs(ctx, newColumnWithType(0, tInt), newColumnWithType(0, tInt), newColumnWithType(0, tInt))
	c.Assert(chs, Equals, charset.CharsetBin)
	c.Assert(coll, Equals, charset.CollationBin)
	c.Assert(flen, Equals, types.UnspecifiedLength)
}
