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
	"bytes"
	"math"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
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
	start := 1000
	samples[0].SetInt64(0)
	for i := 1; i < start; i++ {
		samples[i].SetInt64(2)
	}
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
	rc.data[0].SetInt64(0)
	for i := 1; i < start; i++ {
		rc.data[i].SetInt64(2)
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

func buildPK(ctx context.Context, numBuckets, id int64, records ast.RecordSet) (int64, *Histogram, error) {
	b := NewSortedBuilder(ctx.GetSessionVars().StmtCtx, numBuckets, id)
	for {
		row, err := records.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		err = b.Iterate(row.Data[0])
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}
	return b.Count, b.hist, nil
}

func (s *testStatisticsSuite) TestBuild(c *C) {
	bucketCount := int64(256)
	_, ndv, _ := buildFMSketch(s.rc.(*recordSet).data, 1000)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	col, err := BuildColumn(ctx, bucketCount, 2, ndv, s.count, 0, s.samples)
	c.Check(err, IsNil)
	c.Check(len(col.Buckets), Equals, 232)
	count, err := col.equalRowCount(sc, types.NewIntDatum(1000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 0)
	count, err = col.lessRowCount(sc, types.NewIntDatum(1000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 10000)
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
	count, err = col.lessRowCount(sc, types.NewIntDatum(1))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 9)

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
	count, err = col.lessRowCount(sc, encodeKey(types.NewIntDatum(0)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 0)

	s.pk.(*recordSet).cursor = 0
	tblCount, col, err = buildPK(ctx, bucketCount, 4, ast.RecordSet(s.pk))
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
	count, err = col.lessAndEqRowCount(sc, types.Datum{})
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 0)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(1001))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99231)
	count, err = col.lessRowCount(sc, types.NewIntDatum(99999))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99999)
}

func (s *testStatisticsSuite) TestHistogramProtoConversion(c *C) {
	ctx := mock.NewContext()
	s.rc.Close()
	tblCount, col, err := BuildIndex(ctx, 256, 1, ast.RecordSet(s.rc))
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)

	p := HistogramToProto(col)
	h := HistogramFromProto(p)
	c.Assert(col.NDV, Equals, h.NDV)
	c.Assert(len(col.Buckets), Equals, len(h.Buckets))
	for i, bkt := range col.Buckets {
		c.Assert(bkt.Count, Equals, h.Buckets[i].Count)
		c.Assert(bkt.Repeats, Equals, h.Buckets[i].Repeats)
		c.Assert(bytes.Equal(bkt.LowerBound.GetBytes(), h.Buckets[i].LowerBound.GetBytes()), IsTrue)
		c.Assert(bytes.Equal(bkt.UpperBound.GetBytes(), h.Buckets[i].UpperBound.GetBytes()), IsTrue)
	}
}

func mockHistogram(lower, num int64) *Histogram {
	h := &Histogram{
		NDV: num,
	}
	for i := int64(0); i < num; i++ {
		bkt := Bucket{
			LowerBound: types.NewIntDatum(lower + i),
			UpperBound: types.NewIntDatum(lower + i),
			Count:      i + 1,
			Repeats:    1,
		}
		h.Buckets = append(h.Buckets, bkt)
	}
	return h
}

func (s *testStatisticsSuite) TestMergeHistogram(c *C) {
	tests := []struct {
		leftLower  int64
		leftNum    int64
		rightLower int64
		rightNum   int64
		bucketNum  int
		ndv        int64
		count      int64
		lower      int64
		upper      int64
	}{
		{
			leftLower:  0,
			leftNum:    0,
			rightLower: 0,
			rightNum:   1,
			bucketNum:  1,
			ndv:        1,
			count:      1,
			lower:      0,
			upper:      0,
		},
		{
			leftLower:  0,
			leftNum:    200,
			rightLower: 200,
			rightNum:   200,
			bucketNum:  200,
			ndv:        400,
			count:      400,
			lower:      0,
			upper:      399,
		},
		{
			leftLower:  0,
			leftNum:    200,
			rightLower: 199,
			rightNum:   200,
			bucketNum:  200,
			ndv:        399,
			count:      400,
			lower:      0,
			upper:      398,
		},
	}
	sc := mock.NewContext().GetSessionVars().StmtCtx
	bucketCount := 256
	for _, t := range tests {
		lh := mockHistogram(t.leftLower, t.leftNum)
		rh := mockHistogram(t.rightLower, t.rightNum)
		h, err := MergeHistograms(sc, lh, rh, bucketCount)
		c.Assert(err, IsNil)
		c.Assert(h.NDV, Equals, t.ndv)
		c.Assert(len(h.Buckets), Equals, t.bucketNum)
		c.Assert(h.Buckets[len(h.Buckets)-1].Count, Equals, t.count)
		cmp, err := h.Buckets[0].LowerBound.CompareDatum(sc, types.NewIntDatum(t.lower))
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
		cmp, err = h.Buckets[len(h.Buckets)-1].UpperBound.CompareDatum(sc, types.NewIntDatum(t.upper))
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
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
	c.Assert(int(count), Equals, 3333)
	count, err = tbl.ColumnEqualRowCount(sc, types.NewIntDatum(1000), colInfo)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10)
	count, err = tbl.ColumnBetweenRowCount(sc, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 250)
}

func (s *testStatisticsSuite) TestColumnRange(c *C) {
	bucketCount := int64(256)
	_, ndv, _ := buildFMSketch(s.rc.(*recordSet).data, 1000)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	hg, err := BuildColumn(ctx, bucketCount, 5, ndv, s.count, 0, s.samples)
	c.Check(err, IsNil)
	col := &Column{Histogram: *hg}
	tbl := &Table{
		Count:   int64(col.totalRowCount()),
		Columns: make(map[int64]*Column),
	}
	ran := []*types.ColumnRange{{
		Low:  types.Datum{},
		High: types.MaxValueDatum(),
	}}
	count, err := tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].Low = types.MinNotNullDatum()
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 99900)
	ran[0].Low = types.NewIntDatum(1000)
	ran[0].LowExcl = true
	ran[0].High = types.NewIntDatum(2000)
	ran[0].HighExcl = true
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].LowExcl = false
	ran[0].HighExcl = false
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].Low = ran[0].High
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100)

	tbl.Columns[0] = col
	ran[0].Low = types.Datum{}
	ran[0].High = types.MaxValueDatum()
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].Low = types.NewIntDatum(1000)
	ran[0].LowExcl = true
	ran[0].High = types.NewIntDatum(2000)
	ran[0].HighExcl = true
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 9964)
	ran[0].LowExcl = false
	ran[0].HighExcl = false
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 9965)
	ran[0].Low = ran[0].High
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)
}

func (s *testStatisticsSuite) TestIntColumnRanges(c *C) {
	bucketCount := int64(256)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	s.pk.(*recordSet).cursor = 0
	rowCount, hg, err := buildPK(ctx, bucketCount, 0, s.pk)
	c.Check(err, IsNil)
	c.Check(rowCount, Equals, int64(100000))
	col := &Column{Histogram: *hg}
	tbl := &Table{
		Count:   int64(col.totalRowCount()),
		Columns: make(map[int64]*Column),
	}
	ran := []types.IntColumnRange{{
		LowVal:  math.MinInt64,
		HighVal: math.MaxInt64,
	}}
	count, err := tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal = 1000
	ran[0].HighVal = 2000
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1000)
	ran[0].LowVal = 1001
	ran[0].HighVal = 1999
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 998)
	ran[0].LowVal = 1000
	ran[0].HighVal = 1000
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100)

	tbl.Columns[0] = col
	ran[0].LowVal = math.MinInt64
	ran[0].HighVal = math.MaxInt64
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal = 1000
	ran[0].HighVal = 2000
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1000)
	ran[0].LowVal = 1001
	ran[0].HighVal = 1999
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 998)
	ran[0].LowVal = 1000
	ran[0].HighVal = 1000
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)
}
