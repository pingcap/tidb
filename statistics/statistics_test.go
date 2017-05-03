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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatisticsSuite{})

type testStatisticsSuite struct {
	count   int64
	samples []types.Datum
	rc      ast.RecordSet
	pk      ast.RecordSet
}

type dataTable struct {
	count   int64
	samples []types.Datum
}

type recordSet struct {
	data   []types.Datum
	count  int64
	cursor int64
}

func (r *recordSet) Fields() ([]*ast.ResultField, error) {
	return nil, nil
}

func (r *recordSet) Next() (*ast.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return &ast.Row{Data: []types.Datum{r.data[r.cursor-1]}}, nil
}

func (r *recordSet) Close() error {
	r.cursor = 0
	return nil
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

	rc := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
	}
	for i := int64(start); i < rc.count; i++ {
		rc.data[i].SetInt64(int64(i))
	}
	for i := int64(start); i < rc.count; i += 3 {
		rc.data[i].SetInt64(rc.data[i].GetInt64() + 1)
	}
	for i := int64(start); i < rc.count; i += 5 {
		rc.data[i].SetInt64(rc.data[i].GetInt64() + 2)
	}
	err = types.SortDatums(sc, rc.data)
	c.Check(err, IsNil)
	s.rc = rc

	pk := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
	}
	for i := int64(0); i < rc.count; i++ {
		pk.data[i].SetInt64(int64(i))
	}
	s.pk = pk
}

func encodeKey(key types.Datum) types.Datum {
	bytes, _ := codec.EncodeKey(nil, key)
	return types.NewBytesDatum(bytes)
}

func (s *testStatisticsSuite) TestBuild(c *C) {
	bucketCount := int64(256)
	_, ndv, _ := buildFMSketch(s.rc.(*recordSet).data, 1000)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	col, err := BuildColumn(ctx, bucketCount, 2, ndv, s.count, s.samples)
	c.Check(err, IsNil)
	c.Check(len(col.Buckets), Equals, 232)
	count, err := col.equalRowCount(sc, types.NewIntDatum(1000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 1)
	count, err = col.lessRowCount(sc, types.NewIntDatum(2000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 19964)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(2000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 80034)
	count, err = col.lessRowCount(sc, types.NewIntDatum(200000000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 100000)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(200000000))
	c.Check(err, IsNil)
	c.Check(count, Equals, 0.0)
	count, err = col.equalRowCount(sc, types.NewIntDatum(200000000))
	c.Check(err, IsNil)
	c.Check(count, Equals, 0.0)
	count, err = col.betweenRowCount(sc, types.NewIntDatum(3000), types.NewIntDatum(3500))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 5075)

	tblCount, col, err := BuildIndex(ctx, bucketCount, 1, ast.RecordSet(s.rc))
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)
	count, err = col.equalRowCount(sc, encodeKey(types.NewIntDatum(10000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 1)
	count, err = col.lessRowCount(sc, encodeKey(types.NewIntDatum(20000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 19983)
	count, err = col.betweenRowCount(sc, encodeKey(types.NewIntDatum(30000)), encodeKey(types.NewIntDatum(35000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 4618)

	tblCount, col, err = BuildPK(ctx, bucketCount, 4, ast.RecordSet(s.pk))
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)
	count, err = col.equalRowCount(sc, types.NewIntDatum(10000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 1)
	count, err = col.lessRowCount(sc, types.NewIntDatum(20000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 20223)
	count, err = col.betweenRowCount(sc, types.NewIntDatum(30000), types.NewIntDatum(35000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 5120)
	count, err = col.greaterAndEqRowCount(sc, types.NewIntDatum(1001))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99232)
	count, err = col.lessAndEqRowCount(sc, types.NewIntDatum(99999))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 100000)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(1001))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99231)
	count, err = col.lessRowCount(sc, types.NewIntDatum(99999))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99999)
}

func (s *testStatisticsSuite) TestPseudoTable(c *C) {
	ti := &model.TableInfo{}
	colInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	ti.Columns = append(ti.Columns, colInfo)
	tbl := PseudoTable(ti.ID)
	c.Assert(tbl.Count, Greater, int64(0))
	sc := new(variable.StatementContext)
	count, err := tbl.ColumnLessRowCount(sc, types.NewIntDatum(100), colInfo)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 3333333)
	count, err = tbl.ColumnEqualRowCount(sc, types.NewIntDatum(1000), colInfo)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10000)
	count, err = tbl.ColumnBetweenRowCount(sc, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 250000)
}
