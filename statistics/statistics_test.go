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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	goctx "golang.org/x/net/context"
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
	fields []*ast.ResultField
}

func (r *recordSet) Fields() []*ast.ResultField {
	return r.fields
}

func (r *recordSet) setFields(tps ...uint8) {
	r.fields = make([]*ast.ResultField, len(tps))
	for i := 0; i < len(tps); i++ {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = *types.NewFieldType(tps[i])
		r.fields[i] = rf
	}
}

func (r *recordSet) Next(goctx.Context) (types.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return types.DatumRow{r.data[r.cursor-1]}, nil
}

func (r *recordSet) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	return nil
}

func (r *recordSet) NewChunk() *chunk.Chunk {
	return nil
}

func (r *recordSet) SupportChunk() bool {
	return false
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
	sc := new(stmtctx.StatementContext)
	err := types.SortDatums(sc, samples)
	c.Check(err, IsNil)
	s.samples = samples

	rc := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
	}
	rc.setFields(mysql.TypeLonglong)
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
	pk.setFields(mysql.TypeLonglong)
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
	goCtx := goctx.Background()
	for {
		row, err := records.Next(goCtx)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		datums := ast.RowToDatums(row, records.Fields())
		err = b.Iterate(datums[0])
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}
	return b.Count, b.hist, nil
}

func buildIndex(ctx context.Context, numBuckets, id int64, records ast.RecordSet) (int64, *Histogram, *CMSketch, error) {
	b := NewSortedBuilder(ctx.GetSessionVars().StmtCtx, numBuckets, id)
	cms := NewCMSketch(8, 2048)
	goCtx := goctx.Background()
	for {
		row, err := records.Next(goCtx)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		datums := ast.RowToDatums(row, records.Fields())
		bytes, err := codec.EncodeKey(nil, datums...)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		data := types.NewBytesDatum(bytes)
		err = b.Iterate(data)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		cms.InsertBytes(bytes)
	}
	return b.Count, b.Hist(), cms, nil
}

func calculateScalar(hist *Histogram) {
	for i, bkt := range hist.Buckets {
		bkt.lowerScalar, bkt.upperScalar, bkt.commonPfxLen = preCalculateDatumScalar(&bkt.LowerBound, &bkt.UpperBound)
		hist.Buckets[i] = bkt
	}
}

func checkRepeats(c *C, hg *Histogram) {
	for _, bkt := range hg.Buckets {
		c.Assert(bkt.Repeats, Greater, int64(0))
	}
}

func (s *testStatisticsSuite) TestBuild(c *C) {
	bucketCount := int64(256)
	sketch, _, _ := buildFMSketch(s.rc.(*recordSet).data, 1000)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	collector := &SampleCollector{
		Count:     s.count,
		NullCount: 0,
		Samples:   s.samples,
		FMSketch:  sketch,
	}
	col, err := BuildColumn(ctx, bucketCount, 2, collector)
	checkRepeats(c, col)
	calculateScalar(col)
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
	c.Check(int(count), Equals, 19995)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(2000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 80003)
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
	c.Check(int(count), Equals, 5008)
	count, err = col.lessRowCount(sc, types.NewIntDatum(1))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 9)

	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.pk,
		ColLen:          1,
		PkID:            -1,
		MaxSampleSize:   1000,
		MaxFMSketchSize: 1000,
	}
	s.pk.Close()
	collectors, _, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(len(collectors), Equals, 1)
	col, err = BuildColumn(mock.NewContext(), 256, 2, collectors[0])
	c.Assert(err, IsNil)
	checkRepeats(c, col)

	tblCount, col, _, err := buildIndex(ctx, bucketCount, 1, ast.RecordSet(s.rc))
	checkRepeats(c, col)
	calculateScalar(col)
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)
	count, err = col.equalRowCount(sc, encodeKey(types.NewIntDatum(10000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 1)
	count, err = col.lessRowCount(sc, encodeKey(types.NewIntDatum(20000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 19999)
	count, err = col.betweenRowCount(sc, encodeKey(types.NewIntDatum(30000)), encodeKey(types.NewIntDatum(35000)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 4999)
	count, err = col.lessRowCount(sc, encodeKey(types.NewIntDatum(0)))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 0)

	s.pk.(*recordSet).cursor = 0
	tblCount, col, err = buildPK(ctx, bucketCount, 4, ast.RecordSet(s.pk))
	checkRepeats(c, col)
	calculateScalar(col)
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)
	count, err = col.equalRowCount(sc, types.NewIntDatum(10000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 1)
	count, err = col.lessRowCount(sc, types.NewIntDatum(20000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 20000)
	count, err = col.betweenRowCount(sc, types.NewIntDatum(30000), types.NewIntDatum(35000))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 5000)
	count, err = col.greaterAndEqRowCount(sc, types.NewIntDatum(1001))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 98999)
	count, err = col.lessAndEqRowCount(sc, types.NewIntDatum(99999))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 100000)
	count, err = col.lessAndEqRowCount(sc, types.Datum{})
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 0)
	count, err = col.greaterRowCount(sc, types.NewIntDatum(1001))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 98998)
	count, err = col.lessRowCount(sc, types.NewIntDatum(99999))
	c.Check(err, IsNil)
	c.Check(int(count), Equals, 99999)
}

func (s *testStatisticsSuite) TestHistogramProtoConversion(c *C) {
	ctx := mock.NewContext()
	s.rc.Close()
	tblCount, col, _, err := buildIndex(ctx, 256, 1, ast.RecordSet(s.rc))
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
	}{
		{
			leftLower:  0,
			leftNum:    0,
			rightLower: 0,
			rightNum:   1,
			bucketNum:  1,
			ndv:        1,
		},
		{
			leftLower:  0,
			leftNum:    200,
			rightLower: 200,
			rightNum:   200,
			bucketNum:  200,
			ndv:        400,
		},
		{
			leftLower:  0,
			leftNum:    200,
			rightLower: 199,
			rightNum:   200,
			bucketNum:  200,
			ndv:        399,
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
		c.Assert(h.Buckets[len(h.Buckets)-1].Count, Equals, t.leftNum+t.rightNum)
		expectLower := types.NewIntDatum(t.leftLower)
		cmp, err := h.Buckets[0].LowerBound.CompareDatum(sc, &expectLower)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
		expectUpper := types.NewIntDatum(t.rightLower + t.rightNum - 1)
		cmp, err = h.Buckets[len(h.Buckets)-1].UpperBound.CompareDatum(sc, &expectUpper)
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
	sc := new(stmtctx.StatementContext)
	count, err := tbl.ColumnLessRowCount(sc, types.NewIntDatum(100), colInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 3333)
	count, err = tbl.ColumnEqualRowCount(sc, types.NewIntDatum(1000), colInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10)
	count, err = tbl.ColumnBetweenRowCount(sc, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 250)
}

func buildCMSketch(values []types.Datum) *CMSketch {
	cms := NewCMSketch(8, 2048)
	for _, val := range values {
		cms.insert(&val)
	}
	return cms
}

func (s *testStatisticsSuite) TestColumnRange(c *C) {
	bucketCount := int64(256)
	sketch, _, _ := buildFMSketch(s.rc.(*recordSet).data, 1000)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	collector := &SampleCollector{
		Count:     s.count,
		NullCount: 0,
		Samples:   s.samples,
		FMSketch:  sketch,
	}
	hg, err := BuildColumn(ctx, bucketCount, 2, collector)
	calculateScalar(hg)
	c.Check(err, IsNil)
	col := &Column{Histogram: *hg, CMSketch: buildCMSketch(s.rc.(*recordSet).data)}
	tbl := &Table{
		Count:   int64(col.totalRowCount()),
		Columns: make(map[int64]*Column),
	}
	ran := []*ranger.ColumnRange{{
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
	c.Assert(int(count), Equals, 9994)
	ran[0].LowExcl = false
	ran[0].HighExcl = false
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 9996)
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
	calculateScalar(hg)
	c.Check(err, IsNil)
	c.Check(rowCount, Equals, int64(100000))
	col := &Column{Histogram: *hg}
	tbl := &Table{
		Count:   int64(col.totalRowCount()),
		Columns: make(map[int64]*Column),
	}
	ran := []ranger.IntColumnRange{{
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

func (s *testStatisticsSuite) TestIndexRanges(c *C) {
	bucketCount := int64(256)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	s.rc.(*recordSet).cursor = 0
	rowCount, hg, cms, err := buildIndex(ctx, bucketCount, 0, s.rc)
	calculateScalar(hg)
	c.Check(err, IsNil)
	c.Check(rowCount, Equals, int64(100000))
	idxInfo := &model.IndexInfo{Columns: []*model.IndexColumn{{Offset: 0}}}
	idx := &Index{Histogram: *hg, CMSketch: cms, Info: idxInfo}
	tbl := &Table{
		Count:   int64(idx.totalRowCount()),
		Indices: make(map[int64]*Index),
	}
	ran := []*ranger.IndexRange{{
		LowVal:  []types.Datum{types.MinNotNullDatum()},
		HighVal: []types.Datum{types.MaxValueDatum()},
	}}
	count, err := tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 99900)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].HighVal[0] = types.NewIntDatum(2000)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].LowVal[0] = types.NewIntDatum(1001)
	ran[0].HighVal[0] = types.NewIntDatum(1999)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].HighVal[0] = types.NewIntDatum(1000)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100)

	tbl.Indices[0] = idx
	ran[0].LowVal[0] = types.MinNotNullDatum()
	ran[0].HighVal[0] = types.MaxValueDatum()
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].HighVal[0] = types.NewIntDatum(2000)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1000)
	ran[0].LowVal[0] = types.NewIntDatum(1001)
	ran[0].HighVal[0] = types.NewIntDatum(1990)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 989)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].HighVal[0] = types.NewIntDatum(1000)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 0)
}
