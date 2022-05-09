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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func recordSetForWeightSamplingTest(size int) *recordSet {
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

func recordSetForDistributedSamplingTest(size, batch int) []*recordSet {
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

func TestWeightedSampling(t *testing.T) {
	sampleNum := int64(20)
	rowNum := 100
	loopCnt := 1000
	rs := recordSetForWeightSamplingTest(rowNum)
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
		require.NoError(t, err)
		for i := 0; i < int(sampleNum); i++ {
			a := collector.Base().Samples[i].Columns[0].GetInt64()
			itemCnt[a]++
		}
		require.Nil(t, rs.Close())
	}
	expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
	delta := 0.5
	for _, cnt := range itemCnt {
		if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
			require.Truef(t, false, "The frequency %v is exceed the Chernoff Bound", cnt)
		}
	}
}

func TestDistributedWeightedSampling(t *testing.T) {
	sampleNum := int64(10)
	rowNum := 100
	loopCnt := 1500
	batch := 5
	sets := recordSetForDistributedSamplingTest(rowNum, batch)
	sc := mock.NewContext().GetSessionVars().StmtCtx
	// The loop which is commented out is used for stability test.
	// This test can run 800 times in a row without any failure.
	// for x := 0; x < 800; x++ {
	itemCnt := make([]int, rowNum)
	for loopI := 1; loopI < loopCnt; loopI++ {
		rootRowCollector := NewReservoirRowSampleCollector(int(sampleNum), 1)
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
			require.NoError(t, err)
			rootRowCollector.MergeCollector(collector)
			require.Nil(t, sets[i].Close())
		}
		for _, sample := range rootRowCollector.Samples {
			itemCnt[sample.Columns[0].GetInt64()]++
		}
	}
	expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
	delta := 0.5
	for _, cnt := range itemCnt {
		if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
			require.Truef(t, false, "the frequency %v is exceed the Chernoff Bound", cnt)
		}
	}
}

func TestBuildStatsOnRowSample(t *testing.T) {
	ctx := mock.NewContext()
	sketch := NewFMSketch(1000)
	data := make([]*SampleItem, 0, 8)
	for i := 1; i <= 1000; i++ {
		d := types.NewIntDatum(int64(i))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 10; i++ {
		d := types.NewIntDatum(int64(2))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 7; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 5; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 3; i++ {
		d := types.NewIntDatum(int64(11))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
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
	require.Nilf(t, err, "%+v", err)
	topNStr, err := topN.DecodedString(ctx, []byte{tp.GetType()})
	require.NoError(t, err)
	require.Equal(t, "TopN{length: 4, [(2, 10), (4, 7), (7, 5), (11, 3)]}", topNStr)
	require.Equal(t, "column:1 ndv:1000 totColSize:8168\n"+
		"num: 200 lower_bound: 1 upper_bound: 204 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 205 upper_bound: 404 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 405 upper_bound: 604 repeats: 1 ndv: 0\n"+
		"num: 200 lower_bound: 605 upper_bound: 804 repeats: 1 ndv: 0\n"+
		"num: 196 lower_bound: 805 upper_bound: 1000 repeats: 1 ndv: 0", hist.ToString(0))
}

type testSampleSuite struct {
	count int
	rs    sqlexec.RecordSet
}

func TestSampleSerial(t *testing.T) {
	s := createTestSampleSuite()
	t.Run("SubTestCollectColumnStats", SubTestCollectColumnStats(s))
	t.Run("SubTestMergeSampleCollector", SubTestMergeSampleCollector(s))
	t.Run("SubTestCollectorProtoConversion", SubTestCollectorProtoConversion(s))
}

func createTestSampleSuite() *testSampleSuite {
	s := new(testSampleSuite)
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
	return s
}

func SubTestCollectColumnStats(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
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
		require.Nil(t, s.rs.Close())
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)

		require.Equal(t, int64(s.count), collectors[0].NullCount+collectors[0].Count)
		require.Equal(t, int64(6232), collectors[0].FMSketch.NDV())
		require.Equal(t, uint64(collectors[0].Count), collectors[0].CMSketch.TotalCount())
		require.Equal(t, int64(s.count), pkBuilder.Count)
		require.Equal(t, int64(s.count), pkBuilder.Hist().NDV)
	}
}

func SubTestMergeSampleCollector(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
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
		require.Nil(t, s.rs.Close())
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)
		require.Nil(t, pkBuilder)
		require.Len(t, collectors, 2)
		collectors[0].IsMerger = true
		collectors[0].MergeSampleCollector(sc, collectors[1])
		require.Equal(t, int64(9280), collectors[0].FMSketch.NDV())
		require.Len(t, collectors[0].Samples, 1000)
		require.Equal(t, int64(1000), collectors[0].NullCount)
		require.Equal(t, int64(19000), collectors[0].Count)
		require.Equal(t, uint64(collectors[0].Count), collectors[0].CMSketch.TotalCount())
	}
}

func SubTestCollectorProtoConversion(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
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
		require.Nil(t, s.rs.Close())
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)
		require.Nil(t, pkBuilder)
		for _, collector := range collectors {
			p := SampleCollectorToProto(collector)
			s := SampleCollectorFromProto(p)
			require.Equal(t, s.Count, collector.Count)
			require.Equal(t, s.NullCount, collector.NullCount)
			require.Equal(t, s.CMSketch.TotalCount(), collector.CMSketch.TotalCount())
			require.Equal(t, s.FMSketch.NDV(), collector.FMSketch.NDV())
			require.Equal(t, s.TotalSize, collector.TotalSize)
			require.Equal(t, len(s.Samples), len(collector.Samples))
		}
	}
}
