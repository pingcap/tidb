// Copyright 2018 PingCAP, Inc.
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

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util"
	"github.com/pingcap/tidb/v4/util/collate"
	"github.com/pingcap/tidb/v4/util/testleak"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = SerialSuites(&testDistsqlSuite{})

type testDistsqlSuite struct{}

func (s *testDistsqlSuite) TestColumnToProto(c *C) {
	defer testleak.AfterTest(c)()
	// Make sure the Flag is set in tipb.ColumnInfo
	tp := types.NewFieldType(mysql.TypeLong)
	tp.Flag = 10
	tp.Collate = "utf8_bin"
	col := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc := util.ColumnToProto(col)
	expect := &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: 83, ColumnLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	c.Assert(pc, DeepEquals, expect)

	cols := []*model.ColumnInfo{col, col}
	pcs := util.ColumnsToProto(cols, false)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}
	pcs = util.ColumnsToProto(cols, true)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}

	// Make sure the collation ID is successfully set.
	tp = types.NewFieldType(mysql.TypeVarchar)
	tp.Flag = 10
	tp.Collate = "latin1_swedish_ci"
	col1 := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = util.ColumnToProto(col1)
	c.Assert(pc.Collation, Equals, int32(8))

	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	pc = util.ColumnToProto(col)
	expect = &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: -83, ColumnLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	c.Assert(pc, DeepEquals, expect)
	pcs = util.ColumnsToProto(cols, true)
	for _, v := range pcs {
		c.Assert(v.Collation, Equals, int32(-83))
	}
	pc = util.ColumnToProto(col1)
	c.Assert(pc.Collation, Equals, int32(-8))
}
