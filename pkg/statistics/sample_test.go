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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func recordSetForWeightSamplingTest(size int) *recordSet {
	r := &recordSet{
		data:  make([]types.Datum, 0, size),
		count: size,
	}
	for i := range size {
		r.data = append(r.data, types.NewIntDatum(int64(i)))
	}
	r.setFields(mysql.TypeLonglong)
	return r
}

func recordSetForDistributedSamplingTest(size, batch int) []*recordSet {
	sets := make([]*recordSet, 0, batch)
	batchSize := size / batch
	for i := range batch {
		r := &recordSet{
			data:  make([]types.Datum, 0, batchSize),
			count: batchSize,
		}
		for j := range size / batch {
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
	for range loopCnt {
		builder := &RowSampleBuilder{
			Sc:              sc,
			RecordSet:       rs,
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
			Collators:       make([]collate.Collator, 1),
			ColGroups:       nil,
			MaxSampleSize:   int(sampleNum),
			NDVSampleRate:   NDVSampleSkipRate,
			MaxFMSketchSize: 1000,
			Rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		collector, err := builder.Collect()
		require.NoError(t, err)
		for i := range int(sampleNum) {
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
		for i := range batch {
			builder := &RowSampleBuilder{
				Sc:              sc,
				RecordSet:       sets[i],
				ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
				Collators:       make([]collate.Collator, 1),
				ColGroups:       nil,
				MaxSampleSize:   int(sampleNum),
				NDVSampleRate:   NDVSampleSkipRate,
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
	hist, topN, err := BuildHistAndTopN(ctx, 5, 4, 1, collector, tp, true, nil)
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

func TestBuildSampleFullNDV(t *testing.T) {
	// Testing building TopN when the column NDV is larger than the NDV in the sample.
	// This tests the scenario where ndv > sampleNDV in BuildHistAndTopN.
	ctx := mock.NewContext()
	sketch := NewFMSketch(8)
	data := make([]*SampleItem, 0, 8)

	// Create sample data with only 3 distinct values
	for i := 1; i < 41; i++ {
		d := types.NewIntDatum(int64(2))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 31; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i < 25; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: d})
	}

	// Add many more distinct values to the FMSketch to make column NDV > sample NDV
	// This simulates a scenario where the full column has many more distinct values
	// than what's captured in the sample
	for i := 100; i < 200; i++ {
		d := types.NewIntDatum(int64(i))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		// Don't add these to sample data - this creates the discrepancy
	}

	collector := &SampleCollector{
		Samples:   data,
		NullCount: 0,
		Count:     int64(200),
		FMSketch:  sketch,
		TotalSize: int64(len(data)) * 8,
	}

	// Verify that column NDV > sample NDV
	columnNDV := collector.FMSketch.NDV()
	require.Greater(t, columnNDV, int64(3), "Column NDV should be greater than sample NDV (3)")

	tp := types.NewFieldType(mysql.TypeLonglong)
	// Build histogram buckets with 0 buckets, and default 100 TopN.
	_, topN, err := BuildHistAndTopN(ctx, 0, 100, 1, collector, tp, true, nil)
	require.NoError(t, err)
	topNStr, err := topN.DecodedString(ctx, []byte{tp.GetType()})
	require.NoError(t, err)

	// When ndv > sampleNDV, the TopN list gets trimmed to sampleNDV-1 items
	// So with sampleNDV=3, we expect 2 items: max(1, 3-1) = 2
	require.Equal(t, "TopN{length: 2, [(2, 85), (4, 63)]}", topNStr)

	// Verify that the condition ndv > sampleNDV is properly handled
	// The TopN should be trimmed to sampleNDV-1 items when ndv > sampleNDV
	require.Equal(t, 2, len(topN.TopN), "TopN should be trimmed to sampleNDV-1 items when ndv > sampleNDV")
}

func TestRescaleSampledValue(t *testing.T) {
	cases := []struct {
		name       string
		sampled    int64
		population int64
		sample     int64
		expected   int64
	}{
		{"identity when sampled == sample", 50, 100, 50, 100},
		{"scales up linearly", 10, 100, 50, 20},
		{"rounds half up", 1, 3, 2, 2},
		{"rounds down below half", 1, 4, 3, 1},
		{"zero sampled stays zero", 0, 100, 50, 0},
		{"negative sampled clamps to zero", -5, 100, 50, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.expected, rescaleSampledValue(c.sampled, c.population, c.sample))
		})
	}
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
	t.Run("SubTestRowSampleDefaultNDVRate", SubTestRowSampleDefaultNDVRate())
	t.Run("SubTestRowSampleSingletonSketches", SubTestRowSampleSingletonSketches())
	t.Run("SubTestRowSampleRescalesNullCountUnderSubSampling", SubTestRowSampleRescalesNullCountUnderSubSampling())
	t.Run("SubTestSingletonSketchBuildRespectsMaxSize", SubTestSingletonSketchBuildRespectsMaxSize())
	t.Run("SubTestMergePreservesPerRegionSingletonSketches", SubTestMergePreservesPerRegionSingletonSketches())
}

func SubTestRowSampleDefaultNDVRate() func(*testing.T) {
	return func(t *testing.T) {
		rs := recordSetForWeightSamplingTest(100)
		builder := &RowSampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       rs,
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
			Collators:       make([]collate.Collator, 1),
			MaxSampleSize:   10,
			NDVSampleRate:   NDVSampleSkipRate,
			MaxFMSketchSize: 1000,
			Rng:             rand.New(rand.NewSource(1)),
		}
		collector, err := builder.Collect()
		require.NoError(t, err)

		require.Equal(t, int64(100), collector.Base().FMSketches[0].NDV())
		require.Zero(t, collector.Base().SketchSampleCount)
		require.Empty(t, collector.Base().SingletonSketches)

		pbCollector := collector.Base().ToProto()
		require.Empty(t, pbCollector.GetSingletonSketch())
		require.Zero(t, pbCollector.GetSketchSampleCount())
	}
}

func SubTestRowSampleSingletonSketches() func(*testing.T) {
	return func(t *testing.T) {
		intType := types.NewFieldType(mysql.TypeLonglong)
		rs := &sqlexec.SimpleRecordSet{
			ResultFields: []*resolve.ResultField{
				{Column: &model.ColumnInfo{FieldType: *intType}},
				{Column: &model.ColumnInfo{FieldType: *intType}},
			},
			Rows: [][]any{
				{int64(1), int64(10)},
				{int64(2), int64(10)},
				{int64(2), int64(20)},
				{int64(3), int64(20)},
			},
			MaxChunkSize: 32,
		}

		builder := &RowSampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       rs,
			ColsFieldType:   []*types.FieldType{intType, intType},
			Collators:       make([]collate.Collator, 2),
			ColGroups:       [][]int64{{0}, {0, 1}},
			MaxSampleSize:   4,
			NDVSampleRate:   0.5,
			MaxFMSketchSize: 1000,
			// Seed 1 deterministically samples rows [2, 3] under rate=0.5, exercising the
			// "sketch sampling filtered some rows" path while keeping NDVs deterministic.
			Rng: rand.New(rand.NewSource(1)),
		}
		collector, err := builder.Collect()
		require.NoError(t, err)
		base := collector.Base()

		// Only rows (2, 20) and (3, 20) pass the rate=0.5 sketch sampling check.
		require.Equal(t, int64(2), base.SketchSampleCount)
		require.Len(t, base.SingletonSketches, 4)
		require.Equal(t, int64(2), base.SingletonSketches[0].NDV()) // col 0: 2 and 3 are singletons.
		require.Equal(t, int64(0), base.SingletonSketches[1].NDV()) // col 1: 20 repeats.
		// Single-column group [0] is cloned from col 0, so its singletons match.
		require.Equal(t, base.SingletonSketches[0].NDV(), base.SingletonSketches[2].NDV())
		// Multi-column group [0,1]: (2,20) and (3,20) are singleton row pairs.
		require.Equal(t, int64(2), base.SingletonSketches[3].NDV())

		pbCollector := base.ToProto()
		require.Equal(t, int64(2), pbCollector.GetSketchSampleCount())
		require.Len(t, pbCollector.GetSingletonSketch(), 4)

		restored := NewReservoirRowSampleCollector(4, 4)
		restored.FromProto(pbCollector, memory.NewTracker(0, -1))
		require.Equal(t, base.SketchSampleCount, restored.SketchSampleCount)
		require.Len(t, restored.SingletonSketches, 4)
		require.Equal(t, base.SingletonSketches[0].NDV(), restored.SingletonSketches[0].NDV())
		require.Equal(t, base.SingletonSketches[3].NDV(), restored.SingletonSketches[3].NDV())
	}
}

func SubTestRowSampleRescalesNullCountUnderSubSampling() func(*testing.T) {
	return func(t *testing.T) {
		intType := types.NewFieldType(mysql.TypeLonglong)
		rs := &sqlexec.SimpleRecordSet{
			ResultFields: []*resolve.ResultField{
				{Column: &model.ColumnInfo{FieldType: *intType}},
				{Column: &model.ColumnInfo{FieldType: *intType}},
			},
			// Seed 1 + rate 0.5 deterministically samples rows [2, 3].
			// Nulls outside the sampled window are intentionally not "seen" so the
			// rescaled estimate diverges from the true full-table null count — that
			// is the entire point of post-sampling rescaling.
			Rows: [][]any{
				{nil, int64(10)},      // row 0 — not sampled.
				{int64(2), nil},       // row 1 — not sampled.
				{int64(3), int64(20)}, // row 2 — sampled. no nulls.
				{nil, int64(40)},      // row 3 — sampled. col 0 null.
			},
			MaxChunkSize: 32,
		}

		builder := &RowSampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       rs,
			ColsFieldType:   []*types.FieldType{intType, intType},
			Collators:       make([]collate.Collator, 2),
			MaxSampleSize:   4,
			NDVSampleRate:   0.5,
			MaxFMSketchSize: 1000,
			Rng:             rand.New(rand.NewSource(1)),
		}
		collector, err := builder.Collect()
		require.NoError(t, err)
		base := collector.Base()

		// Count is exact by construction (mock store doesn't drop rows), so we
		// don't assert on it here — we only check that NullCount got rescaled.
		// Rows 2 and 3 are sampled (rate=0.5, seed=1), and within that sample
		// col 0 has 1 null and col 1 has 0 nulls. Rescale factor is 4/2 = 2.
		require.Equal(t, int64(2), base.SketchSampleCount)
		require.Equal(t, int64(2), base.NullCount[0])
		require.Equal(t, int64(0), base.NullCount[1])
	}
}

func SubTestSingletonSketchBuildRespectsMaxSize() func(*testing.T) {
	return func(t *testing.T) {
		builder := newSingletonSketchBuilder()
		for i := range 100 {
			builder.insertHashValue(uint64(i))
		}

		require.LessOrEqual(t, len(builder.build(10).hashset), 10)
	}
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
		sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
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

// SubTestMergePreservesPerRegionSingletonSketches checks that merging keeps each
// region's sketches separate (in RegionSketchSummaries) rather than unioning the
// singleton sketches, which would miscount a value that is a singleton in two
// regions as a global singleton.
func SubTestMergePreservesPerRegionSingletonSketches() func(*testing.T) {
	return func(t *testing.T) {
		// Distinct hash values stand in for distinct data values; with maxSize=1000
		// the FM sketch mask stays 0 so NDV is exact.
		const (
			a = uint64(100)
			b = uint64(200)
			c = uint64(300)
		)
		// makeRegion builds a single-column leaf collector that mimics one analyze
		// response: its own NDV sketch and singleton sketch over that region's
		// sub-sample.
		makeRegion := func(ndv, singleton []uint64, sketchSampleCount int64) *ReservoirRowSampleCollector {
			coll := NewReservoirRowSampleCollector(1, 1)
			coll.Base().FMSketches = []*FMSketch{newFMSketchFromHashValues(ndv...)}
			coll.Base().SingletonSketches = []*FMSketch{newFMSketchFromHashValues(singleton...)}
			coll.Base().SketchSampleCount = sketchSampleCount
			return coll
		}

		// `a` is a local singleton in both regions, so it occurs twice globally and
		// is not a global singleton. `b` and `c` are the only global singletons.
		region1 := makeRegion([]uint64{a, b}, []uint64{a, b}, 2)
		region2 := makeRegion([]uint64{a, c}, []uint64{a, c}, 2)

		root := NewReservoirRowSampleCollector(1, 1)
		root.Base().FMSketches = []*FMSketch{NewFMSketch(1000)}
		root.MergeCollector(region1)
		root.MergeCollector(region2)

		// The two regions stay separate instead of collapsing into one sketch.
		require.Len(t, root.Base().RegionSketchSummaries, 2)
		require.Equal(t, int64(4), root.Base().SketchSampleCount)

		ndvSketches := make([]*FMSketch, 0, 2)
		singletonSketches := make([]*FMSketch, 0, 2)
		for _, region := range root.Base().RegionSketchSummaries {
			ndvSketches = append(ndvSketches, region.NDVSketches[0])
			singletonSketches = append(singletonSketches, region.SingletonSketches[0])
		}
		// Per-region keeps `a` out of the global singleton count (it appears in both
		// regions' NDV sketches); only `b` and `c` remain.
		require.Equal(t, uint64(2), EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches))

		// Contrast with the previous behavior: unioning the two regions into a single
		// sketch over-counts the cross-region duplicate `a` as a global singleton.
		unionNDV := newFMSketchFromHashValues(a, b, c)
		unionSingleton := newFMSketchFromHashValues(a, b, c)
		require.Equal(t, uint64(3),
			EstimateGlobalSingletonBySketches([]*FMSketch{unionNDV}, []*FMSketch{unionSingleton}),
			"a union of the regions over-counts the cross-region duplicate")

		// A merged collector contributes its region summaries when merged again, so
		// per-region granularity survives the second (worker -> root) merge level.
		top := NewReservoirRowSampleCollector(1, 1)
		top.Base().FMSketches = []*FMSketch{NewFMSketch(1000)}
		top.MergeCollector(root)
		require.Len(t, top.Base().RegionSketchSummaries, 2)
	}
}
