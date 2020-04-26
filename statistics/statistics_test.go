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
	"context"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatisticsSuite{})

type testStatisticsSuite struct {
	count   int
	samples []*SampleItem
	rc      sqlexec.RecordSet
	pk      sqlexec.RecordSet
}

type recordSet struct {
	firstIsID bool
	data      []types.Datum
	count     int
	cursor    int
	fields    []*ast.ResultField
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

func (r *recordSet) getNext() []types.Datum {
	if r.cursor == r.count {
		return nil
	}
	r.cursor++
	row := make([]types.Datum, 0, len(r.fields))
	if r.firstIsID {
		row = append(row, types.NewIntDatum(int64(r.cursor)))
	}
	row = append(row, r.data[r.cursor-1])
	return row
}

func (r *recordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	row := r.getNext()
	if row != nil {
		for i := 0; i < len(row); i++ {
			req.AppendDatum(i, &row[i])
		}
	}
	return nil
}

func (r *recordSet) NewChunk() *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(r.fields))
	for _, field := range r.fields {
		fields = append(fields, &field.Column.FieldType)
	}
	return chunk.NewChunkWithCapacity(fields, 32)
}

func (r *recordSet) Close() error {
	r.cursor = 0
	return nil
}

func (s *testStatisticsSuite) SetUpSuite(c *C) {
	s.count = 100000
	samples := make([]*SampleItem, 10000)
	for i := 0; i < len(samples); i++ {
		samples[i] = &SampleItem{}
	}
	start := 1000
	samples[0].Value.SetInt64(0)
	for i := 1; i < start; i++ {
		samples[i].Value.SetInt64(2)
	}
	for i := start; i < len(samples); i++ {
		samples[i].Value.SetInt64(int64(i))
	}
	for i := start; i < len(samples); i += 3 {
		samples[i].Value.SetInt64(samples[i].Value.GetInt64() + 1)
	}
	for i := start; i < len(samples); i += 5 {
		samples[i].Value.SetInt64(samples[i].Value.GetInt64() + 2)
	}
	sc := new(stmtctx.StatementContext)
	err := SortSampleItems(sc, samples)
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
	for i := start; i < rc.count; i++ {
		rc.data[i].SetInt64(int64(i))
	}
	for i := start; i < rc.count; i += 3 {
		rc.data[i].SetInt64(rc.data[i].GetInt64() + 1)
	}
	for i := start; i < rc.count; i += 5 {
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
	for i := 0; i < rc.count; i++ {
		pk.data[i].SetInt64(int64(i))
	}
	s.pk = pk
}

func encodeKey(key types.Datum) types.Datum {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf, _ := codec.EncodeKey(sc, nil, key)
	return types.NewBytesDatum(buf)
}

func buildPK(sctx sessionctx.Context, numBuckets, id int64, records sqlexec.RecordSet) (int64, *Histogram, error) {
	b := NewSortedBuilder(sctx.GetSessionVars().StmtCtx, numBuckets, id, types.NewFieldType(mysql.TypeLonglong))
	ctx := context.Background()
	for {
		req := records.NewChunk()
		err := records.Next(ctx, req)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, records.Fields())
			err = b.Iterate(datums[0])
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
		}
	}
	return b.Count, b.hist, nil
}

func buildIndex(sctx sessionctx.Context, numBuckets, id int64, records sqlexec.RecordSet) (int64, *Histogram, *CMSketch, error) {
	b := NewSortedBuilder(sctx.GetSessionVars().StmtCtx, numBuckets, id, types.NewFieldType(mysql.TypeBlob))
	cms := NewCMSketch(8, 2048)
	ctx := context.Background()
	req := records.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err := records.Next(ctx, req)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, records.Fields())
			buf, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, datums...)
			if err != nil {
				return 0, nil, nil, errors.Trace(err)
			}
			data := types.NewBytesDatum(buf)
			err = b.Iterate(data)
			if err != nil {
				return 0, nil, nil, errors.Trace(err)
			}
			cms.InsertBytes(buf)
		}
	}
	return b.Count, b.Hist(), cms, nil
}

func checkRepeats(c *C, hg *Histogram) {
	for _, bkt := range hg.Buckets {
		c.Assert(bkt.Repeat, Greater, int64(0))
	}
}

func (s *testStatisticsSuite) TestBuild(c *C) {
	bucketCount := int64(256)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	sketch, _, err := buildFMSketch(sc, s.rc.(*recordSet).data, 1000)
	c.Assert(err, IsNil)

	collector := &SampleCollector{
		Count:     int64(s.count),
		NullCount: 0,
		Samples:   s.samples,
		FMSketch:  sketch,
	}
	col, err := BuildColumn(ctx, bucketCount, 2, collector, types.NewFieldType(mysql.TypeLonglong))
	c.Check(err, IsNil)
	checkRepeats(c, col)
	col.PreCalculateScalar()
	c.Check(col.Len(), Equals, 226)
	count := col.equalRowCount(types.NewIntDatum(1000))
	c.Check(int(count), Equals, 0)
	count = col.lessRowCount(types.NewIntDatum(1000))
	c.Check(int(count), Equals, 10000)
	count = col.lessRowCount(types.NewIntDatum(2000))
	c.Check(int(count), Equals, 19999)
	count = col.greaterRowCount(types.NewIntDatum(2000))
	c.Check(int(count), Equals, 80000)
	count = col.lessRowCount(types.NewIntDatum(200000000))
	c.Check(int(count), Equals, 100000)
	count = col.greaterRowCount(types.NewIntDatum(200000000))
	c.Check(count, Equals, 0.0)
	count = col.equalRowCount(types.NewIntDatum(200000000))
	c.Check(count, Equals, 0.0)
	count = col.BetweenRowCount(types.NewIntDatum(3000), types.NewIntDatum(3500))
	c.Check(int(count), Equals, 4994)
	count = col.lessRowCount(types.NewIntDatum(1))
	c.Check(int(count), Equals, 9)

	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.pk,
		ColLen:          1,
		MaxSampleSize:   1000,
		MaxFMSketchSize: 1000,
	}
	c.Assert(s.pk.Close(), IsNil)
	collectors, _, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(len(collectors), Equals, 1)
	col, err = BuildColumn(mock.NewContext(), 256, 2, collectors[0], types.NewFieldType(mysql.TypeLonglong))
	c.Assert(err, IsNil)
	checkRepeats(c, col)
	c.Assert(col.Len(), Equals, 250)

	tblCount, col, _, err := buildIndex(ctx, bucketCount, 1, s.rc)
	c.Check(err, IsNil)
	checkRepeats(c, col)
	col.PreCalculateScalar()
	c.Check(int(tblCount), Equals, 100000)
	count = col.equalRowCount(encodeKey(types.NewIntDatum(10000)))
	c.Check(int(count), Equals, 1)
	count = col.lessRowCount(encodeKey(types.NewIntDatum(20000)))
	c.Check(int(count), Equals, 19999)
	count = col.BetweenRowCount(encodeKey(types.NewIntDatum(30000)), encodeKey(types.NewIntDatum(35000)))
	c.Check(int(count), Equals, 4999)
	count = col.BetweenRowCount(encodeKey(types.MinNotNullDatum()), encodeKey(types.NewIntDatum(0)))
	c.Check(int(count), Equals, 0)
	count = col.lessRowCount(encodeKey(types.NewIntDatum(0)))
	c.Check(int(count), Equals, 0)

	s.pk.(*recordSet).cursor = 0
	tblCount, col, err = buildPK(ctx, bucketCount, 4, s.pk)
	c.Check(err, IsNil)
	checkRepeats(c, col)
	col.PreCalculateScalar()
	c.Check(int(tblCount), Equals, 100000)
	count = col.equalRowCount(types.NewIntDatum(10000))
	c.Check(int(count), Equals, 1)
	count = col.lessRowCount(types.NewIntDatum(20000))
	c.Check(int(count), Equals, 20000)
	count = col.BetweenRowCount(types.NewIntDatum(30000), types.NewIntDatum(35000))
	c.Check(int(count), Equals, 5000)
	count = col.greaterRowCount(types.NewIntDatum(1001))
	c.Check(int(count), Equals, 98998)
	count = col.lessRowCount(types.NewIntDatum(99999))
	c.Check(int(count), Equals, 99999)

	datum := types.Datum{}
	datum.SetMysqlJSON(json.BinaryJSON{TypeCode: json.TypeCodeLiteral})
	item := &SampleItem{Value: datum}
	collector = &SampleCollector{
		Count:     1,
		NullCount: 0,
		Samples:   []*SampleItem{item},
		FMSketch:  sketch,
	}
	col, err = BuildColumn(ctx, bucketCount, 2, collector, types.NewFieldType(mysql.TypeJSON))
	c.Assert(err, IsNil)
	c.Assert(col.Len(), Equals, 1)
	c.Assert(col.GetLower(0), DeepEquals, col.GetUpper(0))
}

func (s *testStatisticsSuite) TestHistogramProtoConversion(c *C) {
	ctx := mock.NewContext()
	c.Assert(s.rc.Close(), IsNil)
	tblCount, col, _, err := buildIndex(ctx, 256, 1, s.rc)
	c.Check(err, IsNil)
	c.Check(int(tblCount), Equals, 100000)

	p := HistogramToProto(col)
	h := HistogramFromProto(p)
	c.Assert(HistogramEqual(col, h, true), IsTrue)
}

func mockHistogram(lower, num int64) *Histogram {
	h := NewHistogram(0, num, 0, 0, types.NewFieldType(mysql.TypeLonglong), int(num), 0)
	for i := int64(0); i < num; i++ {
		lower, upper := types.NewIntDatum(lower+i), types.NewIntDatum(lower+i)
		h.AppendBucket(&lower, &upper, i+1, 1)
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
		c.Assert(h.Len(), Equals, t.bucketNum)
		c.Assert(int64(h.TotalRowCount()), Equals, t.leftNum+t.rightNum)
		expectLower := types.NewIntDatum(t.leftLower)
		cmp, err := h.GetLower(0).CompareDatum(sc, &expectLower)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
		expectUpper := types.NewIntDatum(t.rightLower + t.rightNum - 1)
		cmp, err = h.GetUpper(h.Len()-1).CompareDatum(sc, &expectUpper)
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
	tbl := PseudoTable(ti)
	c.Assert(tbl.Count, Greater, int64(0))
	sc := new(stmtctx.StatementContext)
	count := tbl.ColumnLessRowCount(sc, types.NewIntDatum(100), colInfo.ID)
	c.Assert(int(count), Equals, 3333)
	count, err := tbl.ColumnEqualRowCount(sc, types.NewIntDatum(1000), colInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10)
	count = tbl.ColumnBetweenRowCount(sc, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo.ID)
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
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	sketch, _, err := buildFMSketch(sc, s.rc.(*recordSet).data, 1000)
	c.Assert(err, IsNil)

	collector := &SampleCollector{
		Count:     int64(s.count),
		NullCount: 0,
		Samples:   s.samples,
		FMSketch:  sketch,
	}
	hg, err := BuildColumn(ctx, bucketCount, 2, collector, types.NewFieldType(mysql.TypeLonglong))
	hg.PreCalculateScalar()
	c.Check(err, IsNil)
	col := &Column{Histogram: *hg, CMSketch: buildCMSketch(s.rc.(*recordSet).data), Info: &model.ColumnInfo{}}
	tbl := &Table{
		HistColl: HistColl{
			Count:   int64(col.TotalRowCount()),
			Columns: make(map[int64]*Column),
		},
	}
	ran := []*ranger.Range{{
		LowVal:  []types.Datum{{}},
		HighVal: []types.Datum{types.MaxValueDatum()},
	}}
	count, err := tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0] = types.MinNotNullDatum()
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 99900)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].LowExclude = true
	ran[0].HighVal[0] = types.NewIntDatum(2000)
	ran[0].HighExclude = true
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].LowExclude = false
	ran[0].HighExclude = false
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 2500)
	ran[0].LowVal[0] = ran[0].HighVal[0]
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100)

	tbl.Columns[0] = col
	ran[0].LowVal[0] = types.Datum{}
	ran[0].HighVal[0] = types.MaxValueDatum()
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].LowExclude = true
	ran[0].HighVal[0] = types.NewIntDatum(2000)
	ran[0].HighExclude = true
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 9998)
	ran[0].LowExclude = false
	ran[0].HighExclude = false
	count, err = tbl.GetRowCountByColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10000)
	ran[0].LowVal[0] = ran[0].HighVal[0]
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
	hg.PreCalculateScalar()
	c.Check(err, IsNil)
	c.Check(rowCount, Equals, int64(100000))
	col := &Column{Histogram: *hg, Info: &model.ColumnInfo{}}
	tbl := &Table{
		HistColl: HistColl{
			Count:   int64(col.TotalRowCount()),
			Columns: make(map[int64]*Column),
		},
	}
	ran := []*ranger.Range{{
		LowVal:  []types.Datum{types.NewIntDatum(math.MinInt64)},
		HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)},
	}}
	count, err := tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0].SetInt64(1000)
	ran[0].HighVal[0].SetInt64(2000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1000)
	ran[0].LowVal[0].SetInt64(1001)
	ran[0].HighVal[0].SetInt64(1999)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 998)
	ran[0].LowVal[0].SetInt64(1000)
	ran[0].HighVal[0].SetInt64(1000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)

	ran = []*ranger.Range{{
		LowVal:  []types.Datum{types.NewUintDatum(0)},
		HighVal: []types.Datum{types.NewUintDatum(math.MaxUint64)},
	}}
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0].SetUint64(1000)
	ran[0].HighVal[0].SetUint64(2000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1000)
	ran[0].LowVal[0].SetUint64(1001)
	ran[0].HighVal[0].SetUint64(1999)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 998)
	ran[0].LowVal[0].SetUint64(1000)
	ran[0].HighVal[0].SetUint64(1000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)

	tbl.Columns[0] = col
	ran[0].LowVal[0].SetInt64(math.MinInt64)
	ran[0].HighVal[0].SetInt64(math.MaxInt64)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 100000)
	ran[0].LowVal[0].SetInt64(1000)
	ran[0].HighVal[0].SetInt64(2000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1001)
	ran[0].LowVal[0].SetInt64(1001)
	ran[0].HighVal[0].SetInt64(1999)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 999)
	ran[0].LowVal[0].SetInt64(1000)
	ran[0].HighVal[0].SetInt64(1000)
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)

	tbl.Count *= 10
	count, err = tbl.GetRowCountByIntColumnRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 10)
}

func (s *testStatisticsSuite) TestIndexRanges(c *C) {
	bucketCount := int64(256)
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx

	s.rc.(*recordSet).cursor = 0
	rowCount, hg, cms, err := buildIndex(ctx, bucketCount, 0, s.rc)
	hg.PreCalculateScalar()
	c.Check(err, IsNil)
	c.Check(rowCount, Equals, int64(100000))
	idxInfo := &model.IndexInfo{Columns: []*model.IndexColumn{{Offset: 0}}}
	idx := &Index{Histogram: *hg, CMSketch: cms, Info: idxInfo}
	tbl := &Table{
		HistColl: HistColl{
			Count:   int64(idx.TotalRowCount()),
			Indices: make(map[int64]*Index),
		},
	}
	ran := []*ranger.Range{{
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

	tbl.Indices[0] = &Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Offset: 0}}, Unique: true}}
	ran[0].LowVal[0] = types.NewIntDatum(1000)
	ran[0].HighVal[0] = types.NewIntDatum(1000)
	count, err = tbl.GetRowCountByIndexRanges(sc, 0, ran)
	c.Assert(err, IsNil)
	c.Assert(int(count), Equals, 1)

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
