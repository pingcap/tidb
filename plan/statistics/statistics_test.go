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

package statistics

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatisticsSuite{})

type testStatisticsSuite struct {
	count   int64
	samples []types.Datum
}

type dataTable struct {
	count   int64
	samples []types.Datum
}

func (s *testStatisticsSuite) SetUpSuite(c *C) {
	s.count = 100000
	samples := make([]types.Datum, 10000)
	start := 1000 // 1000 values is null
	for i := start; i < len(samples); i++ {
		samples[i].SetInt64(int64(i))
	}
	for i := start; i < len(samples); i += 3 {
		samples[i].SetInt64(samples[i].GetInt64() + 1)
	}
	for i := start; i < len(samples); i += 5 {
		samples[i].SetInt64(samples[i].GetInt64() + 2)
	}
	sc := new(variable.StatementContext)
	err := types.SortDatums(sc, samples)
	c.Check(err, IsNil)
	s.samples = samples
}

func (s *testStatisticsSuite) TestEstimateNDV(c *C) {
	sc := new(variable.StatementContext)
	ndv, err := estimateNDV(sc, s.count, s.samples)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(49792))
}

func (s *testStatisticsSuite) TestTable(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
	}
	columns := []*model.ColumnInfo{
		{
			ID:        2,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tblInfo.Columns = columns
	timestamp := int64(10)
	bucketCount := int64(256)
	sc := new(variable.StatementContext)
	t, err := NewTable(sc, tblInfo, timestamp, s.count, bucketCount, [][]types.Datum{s.samples})
	c.Check(err, IsNil)

	col := t.Columns[0]
	count, err := col.EqualRowCount(sc, types.NewIntDatum(1000))
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))
	count, err = col.LessRowCount(sc, types.NewIntDatum(2000))
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(19955))
	count, err = col.BetweenRowCount(sc, types.NewIntDatum(3000), types.NewIntDatum(3500))
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(5075))

	str := t.String()
	log.Debug(str)
	c.Check(len(str), Greater, 0)

	tpb, err := t.ToPB()
	c.Check(err, IsNil)
	data, err := proto.Marshal(tpb)
	c.Check(err, IsNil)
	ntpb := &TablePB{}
	err = proto.Unmarshal(data, ntpb)
	c.Check(err, IsNil)
	nt, err := TableFromPB(tblInfo, ntpb)
	c.Check(err, IsNil)
	c.Check(nt.String(), Equals, str)
}

func (s *testStatisticsSuite) TestPseudoTable(c *C) {
	ti := &model.TableInfo{}
	ti.Columns = append(ti.Columns, &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	})
	tbl := PseudoTable(ti)
	c.Assert(tbl.Count, Greater, int64(0))
	c.Assert(tbl.TS, Greater, int64(0))
	col := tbl.Columns[0]
	c.Assert(col.ID, Greater, int64(0))
	c.Assert(col.NDV, Greater, int64(0))
	sc := new(variable.StatementContext)
	count, err := col.LessRowCount(sc, types.NewIntDatum(100))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3333333))
	count, err = col.EqualRowCount(sc, types.NewIntDatum(1000))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(10000))
	count, err = col.BetweenRowCount(sc, types.NewIntDatum(1000), types.NewIntDatum(5000))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(250000))
}
