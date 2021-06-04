// Copyright 2017 PingCAP, Inc.
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
	"math/rand"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
)

var _ = Suite(&testSampleSuite{})

type testSampleSuite struct {
	count int
	rs    sqlexec.RecordSet
}

func (s *testSampleSuite) SetUpSuite(c *C) {
	s.count = 10000
	rs := &recordSet{
		data:      make([]types.Datum, s.count),
		count:     s.count,
		cursor:    0,
		firstIsID: true,
	}
	rs.setFields(mysql.TypeLonglong, mysql.TypeLonglong)
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}
	s.rs = rs
}

func (s *testSampleSuite) TestCollectColumnStats(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	builder := SampleBuilder{
		Sc:              sc,
		RecordSet:       s.rs,
		ColLen:          1,
		PkBuilder:       NewSortedBuilder(sc, 256, 1, types.NewFieldType(mysql.TypeLonglong), Version2),
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 1),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(s.count))
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(6232))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
	c.Assert(pkBuilder.Count, Equals, int64(s.count))
	c.Assert(pkBuilder.Hist().NDV, Equals, int64(s.count))
}

func (s *testSampleSuite) TestMergeSampleCollector(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		MaxSampleSize:   1000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 2),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	c.Assert(len(collectors), Equals, 2)
	collectors[0].IsMerger = true
	collectors[0].MergeSampleCollector(sc, collectors[1])
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(9280))
	c.Assert(len(collectors[0].Samples), Equals, 1000)
	c.Assert(collectors[0].NullCount, Equals, int64(1000))
	c.Assert(collectors[0].Count, Equals, int64(19000))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
}

func (s *testSampleSuite) TestCollectorProtoConversion(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 2),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	for _, collector := range collectors {
		p := SampleCollectorToProto(collector)
		s := SampleCollectorFromProto(p)
		c.Assert(collector.Count, Equals, s.Count)
		c.Assert(collector.NullCount, Equals, s.NullCount)
		c.Assert(collector.CMSketch.TotalCount(), Equals, s.CMSketch.TotalCount())
		c.Assert(collector.FMSketch.NDV(), Equals, s.FMSketch.NDV())
		c.Assert(collector.TotalSize, Equals, s.TotalSize)
		c.Assert(len(collector.Samples), Equals, len(s.Samples))
	}
}

func (s *testSampleSuite) recordSetForWeightSamplingTest(size int) *recordSet {
	r := &recordSet{
		data:  make([]types.Datum, 0, size),
		count: size,
	}
	for i := 0; i < size; i++ {
		r.data = append(r.data, types.NewIntDatum(int64(i)))
	}
	r.setFields(mysql.TypeLonglong)
	return r
}

func (s *testSampleSuite) recordSetForDistributedSamplingTest(size, batch int) []*recordSet {
	sets := make([]*recordSet, 0, batch)
	batchSize := size / batch
	for i := 0; i < batch; i++ {
		r := &recordSet{
			data:  make([]types.Datum, 0, batchSize),
			count: batchSize,
		}
		for j := 0; j < size/batch; j++ {
			r.data = append(r.data, types.NewIntDatum(int64(j+batchSize*i)))
		}
		r.setFields(mysql.TypeLonglong)
		sets = append(sets, r)
	}
	return sets
}

func (s *testSampleSuite) TestWeightedSampling(c *C) {
	sampleNum := int64(20)
	rowNum := 100
	loopCnt := 1000
	rs := s.recordSetForWeightSamplingTest(rowNum)
	sc := mock.NewContext().GetSessionVars().StmtCtx
	// The loop which is commented out is used for stability test.
	// This test can run 800 times in a row without any failure.
	// for x := 0; x < 800; x++ {
	itemCnt := make([]int, rowNum)
	for loopI := 0; loopI < loopCnt; loopI++ {
		builder := &RowSampleBuilder{
			Sc:              sc,
			RecordSet:       rs,
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
			Collators:       make([]collate.Collator, 1),
			ColGroups:       nil,
			MaxSampleSize:   int(sampleNum),
			MaxFMSketchSize: 1000,
			Rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		collector, err := builder.Collect()
		c.Assert(err, IsNil)
		for i := 0; i < collector.MaxSampleSize; i++ {
			a := collector.Samples[i].Columns[0].GetInt64()
			itemCnt[a]++
		}
		c.Assert(rs.Close(), IsNil)
	}
	expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
	delta := 0.5
	for _, cnt := range itemCnt {
		if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
			c.Assert(false, IsTrue, Commentf("The frequency %v is exceed the Chernoff Bound", cnt))
		}
	}
	// }
}

func (s *testSampleSuite) TestDistributedWeightedSampling(c *C) {
	sampleNum := int64(10)
	rowNum := 100
	loopCnt := 1500
	batch := 5
	sets := s.recordSetForDistributedSamplingTest(rowNum, batch)
	sc := mock.NewContext().GetSessionVars().StmtCtx
	// The loop which is commented out is used for stability test.
	// This test can run 800 times in a row without any failure.
	// for x := 0; x < 800; x++ {
	itemCnt := make([]int, rowNum)
	for loopI := 1; loopI < loopCnt; loopI++ {
		rootRowCollector := &RowSampleCollector{
			NullCount:     make([]int64, 1),
			FMSketches:    make([]*FMSketch, 0, 1),
			TotalSizes:    make([]int64, 1),
			Samples:       make(WeightedRowSampleHeap, 0, sampleNum),
			MaxSampleSize: int(sampleNum),
		}
		rootRowCollector.FMSketches = append(rootRowCollector.FMSketches, NewFMSketch(1000))
		for i := 0; i < batch; i++ {
			builder := &RowSampleBuilder{
				Sc:              sc,
				RecordSet:       sets[i],
				ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
				Collators:       make([]collate.Collator, 1),
				ColGroups:       nil,
				MaxSampleSize:   int(sampleNum),
				MaxFMSketchSize: 1000,
				Rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
			}
			collector, err := builder.Collect()
			c.Assert(err, IsNil)
			rootRowCollector.MergeCollector(collector)
			c.Assert(sets[i].Close(), IsNil)
		}
		for _, sample := range rootRowCollector.Samples {
			itemCnt[sample.Columns[0].GetInt64()]++
		}
	}
	expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
	delta := 0.5
	for _, cnt := range itemCnt {
		if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
			c.Assert(false, IsTrue, Commentf("the frequency %v is exceed the Chernoff Bound", cnt))
		}
	}
	// }
}

func (s *testSampleSuite) TestBuildStatsOnRowSample(c *C) {
	ctx := mock.NewContext()
	sketch := NewFMSketch(1000)
	data := make([]*SampleItem, 0, 8)
	for i := 1; i <= 1000; i++ {
		d := types.NewIntDatum(int64(i))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		c.Assert(err, IsNil)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 10; i++ {
		d := types.NewIntDatum(int64(2))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		c.Assert(err, IsNil)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 7; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		c.Assert(err, IsNil)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 5; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		c.Assert(err, IsNil)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 3; i++ {
		d := types.NewIntDatum(int64(11))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		c.Assert(err, IsNil)
		data = append(data, &SampleItem{Value: d})
	}
	collector := &SampleCollector{
		Samples:   data,
		NullCount: 0,
		Count:     int64(len(data)),
		FMSketch:  sketch,
		TotalSize: int64(len(data)) * 8,
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	hist, topN, err := BuildHistAndTopN(ctx, 5, 4, 1, collector, tp, true)
	c.Assert(err, IsNil, Commentf("%+v", err))
	topNStr, err := topN.DecodedString(ctx, []byte{tp.Tp})
	c.Assert(err, IsNil)
	c.Assert(topNStr, Equals, "TopN{length: 4, [(2, 10), (4, 7), (7, 5), (11, 3)]}")
	c.Assert(hist.ToString(0), Equals, "column:1 ndv:1000 totColSize:8168\n"+
		"num: 200 lower_bound: 1 upper_bound: 204 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 205 upper_bound: 404 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 405 upper_bound: 604 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 605 upper_bound: 804 repeats: 1 ndv: 0\n"+
		"num: 196 lower_bound: 805 upper_bound: 1000 repeats: 1 ndv: 0",
	)

}
