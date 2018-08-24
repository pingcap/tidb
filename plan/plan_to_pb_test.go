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

package plan

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = Suite(&testDistsqlSuite{})

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
	pc := model.ColumnToProto(col)
	expect := &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: 83, ColumnLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	c.Assert(pc, DeepEquals, expect)

	cols := []*model.ColumnInfo{col, col}
	pcs := model.ColumnsToProto(cols, false)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}
	pcs = model.ColumnsToProto(cols, true)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}

	// Make sure we only convert to supported collate.
	tp = types.NewFieldType(mysql.TypeVarchar)
	tp.Flag = 10
	tp.Collate = "latin1_swedish_ci"
	col = &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = model.ColumnToProto(col)
	c.Assert(pc.Collation, Equals, int32(mysql.DefaultCollationID))
}

func (s *testDistsqlSuite) TestIndexToProto(c *C) {
	defer testleak.AfterTest(c)()
	cols := []*model.ColumnInfo{
		{
			ID:     1,
			Name:   model.NewCIStr("col1"),
			Offset: 1,
		},
		{
			ID:     2,
			Name:   model.NewCIStr("col2"),
			Offset: 2,
		},
	}
	cols[0].Flag |= mysql.PriKeyFlag

	idxCols := []*model.IndexColumn{
		{
			Name:   model.NewCIStr("col1"),
			Offset: 1,
			Length: 1,
		},
		{
			Name:   model.NewCIStr("col1"),
			Offset: 1,
			Length: 1,
		},
	}

	idxInfos := []*model.IndexInfo{
		{
			ID:      1,
			Name:    model.NewCIStr("idx1"),
			Table:   model.NewCIStr("test"),
			Columns: idxCols,
			Unique:  true,
			Primary: true,
		},
		{
			ID:      2,
			Name:    model.NewCIStr("idx2"),
			Table:   model.NewCIStr("test"),
			Columns: idxCols,
			Unique:  true,
			Primary: true,
		},
	}

	tbInfo := model.TableInfo{
		ID:         1,
		Name:       model.NewCIStr("test"),
		Columns:    cols,
		Indices:    idxInfos,
		PKIsHandle: true,
	}

	pIdx := model.IndexToProto(&tbInfo, idxInfos[0])
	c.Assert(pIdx.TableId, Equals, int64(1))
	c.Assert(pIdx.IndexId, Equals, int64(1))
	c.Assert(pIdx.Unique, Equals, true)
}

func (s *testDistsqlSuite) TestIndexScanToProto(c *C) {
	tp := types.NewFieldType(mysql.TypeLong)
	tp.Flag = 10
	tp.Collate = "utf8_bin"

	name := model.NewCIStr("a")
	col := &model.ColumnInfo{
		ID:        1,
		Name:      name,
		State:     model.StatePublic,
		FieldType: *tp,
	}
	idxInfo := &model.IndexInfo{
		ID:    2,
		Name:  name,
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Length: types.UnspecifiedLength},
		},
	}
	p := new(PhysicalIndexScan)
	p.Table = &model.TableInfo{
		ID:      1,
		Columns: []*model.ColumnInfo{col},
		Indices: []*model.IndexInfo{idxInfo},
	}
	p.Index = idxInfo
	p.schema = expression.NewSchema(&expression.Column{
		ID: model.ExtraHandleID,
	})
	pbExec, err := p.ToPB(nil)
	c.Assert(err, IsNil)
	idxScan := pbExec.IdxScan
	pbColumn := idxScan.Columns[0]
	c.Assert(pbColumn.Tp, Equals, int32(mysql.TypeLonglong))
	c.Assert(pbColumn.ColumnId, Equals, int64(model.ExtraHandleID))
	c.Assert(pbColumn.PkHandle, Equals, true)
}
