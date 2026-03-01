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
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

// makeSampleItem creates a ReservoirRowSampleItem with a single int column and given weight.
func makeSampleItem(val int64, weight int64) *ReservoirRowSampleItem {
	return &ReservoirRowSampleItem{
		Columns: []types.Datum{types.NewIntDatum(val)},
		Weight:  weight,
	}
}

// makeCollector builds a ReservoirRowSampleCollector populated with the given
// samples. FMSketches and NullCount are initialised to the correct length so
// that PruneTo can derive totalLen.
func makeCollector(maxSize int, items []*ReservoirRowSampleItem) *ReservoirRowSampleCollector {
	const cols = 1
	c := NewReservoirRowSampleCollector(maxSize, cols)
	c.FMSketches = append(c.FMSketches, NewFMSketch(100))
	c.Count = int64(len(items))
	for _, it := range items {
		c.sampleZippedRow(it)
	}
	return c
}

// extractWeights returns the sorted weights in the collector's sample heap.
func extractWeights(c *ReservoirRowSampleCollector) []int64 {
	w := make([]int64, len(c.Samples))
	for i, s := range c.Samples {
		w[i] = s.Weight
	}
	slices.Sort(w)
	return w
}

func TestPruneToNoOp(t *testing.T) {
	// When targetSize >= len(Samples) we get a shallow clone.
	items := []*ReservoirRowSampleItem{
		makeSampleItem(1, 100),
		makeSampleItem(2, 200),
		makeSampleItem(3, 300),
	}
	orig := makeCollector(10, items)
	orig.NullCount[0] = 42
	orig.TotalSizes[0] = 999

	pruned := orig.PruneTo(10) // targetSize > len(samples)=3
	require.Len(t, pruned.Samples, 3)

	// Metadata is shared.
	require.Equal(t, orig.Count, pruned.Count)
	require.Equal(t, orig.NullCount[0], pruned.NullCount[0])
	require.Equal(t, orig.TotalSizes[0], pruned.TotalSizes[0])
	require.Same(t, orig.FMSketches[0], pruned.FMSketches[0])

	// MaxSampleSize preserved from original.
	require.Equal(t, orig.MaxSampleSize, pruned.MaxSampleSize)

	// Sample slice is independent — mutating pruned does not affect original.
	pruned.Samples[0] = makeSampleItem(99, 9999)
	require.NotEqual(t, int64(99), orig.Samples[0].Columns[0].GetInt64())
}

func TestPruneToExact(t *testing.T) {
	// targetSize == len(Samples): clone path, not pruning path.
	items := []*ReservoirRowSampleItem{
		makeSampleItem(1, 10),
		makeSampleItem(2, 20),
	}
	orig := makeCollector(5, items)
	pruned := orig.PruneTo(2)
	require.Len(t, pruned.Samples, 2)
	require.Equal(t, extractWeights(orig), extractWeights(pruned))
}

func TestPruneToActualPruning(t *testing.T) {
	// Build a collector with 10 samples (weights 1..10), prune to 3.
	// The 3 highest-weight items (8, 9, 10) must survive.
	items := make([]*ReservoirRowSampleItem, 10)
	for i := range 10 {
		items[i] = makeSampleItem(int64(i), int64(i+1))
	}
	orig := makeCollector(10, items)
	require.Len(t, orig.Samples, 10)

	pruned := orig.PruneTo(3)
	require.Len(t, pruned.Samples, 3)
	require.Equal(t, 3, pruned.MaxSampleSize)

	// The surviving weights must be the top-3.
	weights := extractWeights(pruned)
	require.Equal(t, []int64{8, 9, 10}, weights)

	// Metadata is carried over.
	require.Equal(t, orig.Count, pruned.Count)
	require.Same(t, orig.FMSketches[0], pruned.FMSketches[0])
}

func TestPruneToSingleSample(t *testing.T) {
	// Prune to 1: only the maximum weight survives.
	items := make([]*ReservoirRowSampleItem, 5)
	for i := range 5 {
		items[i] = makeSampleItem(int64(i), int64((i+1)*100))
	}
	orig := makeCollector(5, items)

	pruned := orig.PruneTo(1)
	require.Len(t, pruned.Samples, 1)
	require.Equal(t, int64(500), pruned.Samples[0].Weight)
}

func TestPruneToEmptyCollector(t *testing.T) {
	// An empty collector should return an empty clone.
	orig := makeCollector(10, nil)
	pruned := orig.PruneTo(5)
	require.Len(t, pruned.Samples, 0)
	require.Equal(t, int64(0), pruned.Count)
}

func TestPruneToZeroTarget(t *testing.T) {
	// targetSize=0 means no samples survive.
	items := []*ReservoirRowSampleItem{
		makeSampleItem(1, 100),
		makeSampleItem(2, 200),
	}
	orig := makeCollector(5, items)
	pruned := orig.PruneTo(0)
	require.Len(t, pruned.Samples, 0)
	require.Equal(t, 0, pruned.MaxSampleSize)
	// Count/metadata still carried.
	require.Equal(t, orig.Count, pruned.Count)
}

func TestPruneToDuplicateWeights(t *testing.T) {
	// All samples have the same weight. Pruning should keep exactly targetSize items.
	items := make([]*ReservoirRowSampleItem, 8)
	for i := range 8 {
		items[i] = makeSampleItem(int64(i), 42)
	}
	orig := makeCollector(8, items)
	pruned := orig.PruneTo(3)
	require.Len(t, pruned.Samples, 3)
	for _, s := range pruned.Samples {
		require.Equal(t, int64(42), s.Weight)
	}
}

func TestPruneToPreservesColumnData(t *testing.T) {
	// Verify that column data in surviving samples is correct, not corrupted.
	items := []*ReservoirRowSampleItem{
		{Columns: []types.Datum{types.NewIntDatum(100), types.NewStringDatum("hello")}, Weight: 1},
		{Columns: []types.Datum{types.NewIntDatum(200), types.NewStringDatum("world")}, Weight: 10},
		{Columns: []types.Datum{types.NewIntDatum(300), types.NewStringDatum("foo")}, Weight: 5},
	}
	c := NewReservoirRowSampleCollector(3, 2)
	c.FMSketches = []*FMSketch{NewFMSketch(10), NewFMSketch(10)}
	c.Count = 3
	for _, it := range items {
		c.sampleZippedRow(it)
	}

	pruned := c.PruneTo(1)
	require.Len(t, pruned.Samples, 1)
	// Weight 10 should survive.
	require.Equal(t, int64(10), pruned.Samples[0].Weight)
	require.Equal(t, int64(200), pruned.Samples[0].Columns[0].GetInt64())
	require.Equal(t, "world", pruned.Samples[0].Columns[1].GetString())
}

func TestPruneToIdempotent(t *testing.T) {
	// Pruning twice to the same size should produce equivalent results.
	items := make([]*ReservoirRowSampleItem, 20)
	for i := range 20 {
		items[i] = makeSampleItem(int64(i), int64(i*7+3))
	}
	orig := makeCollector(20, items)

	pruned1 := orig.PruneTo(5)
	pruned2 := pruned1.PruneTo(5)
	require.Equal(t, extractWeights(pruned1), extractWeights(pruned2))
}

func TestPruneToDoesNotMutateOriginal(t *testing.T) {
	items := make([]*ReservoirRowSampleItem, 6)
	for i := range 6 {
		items[i] = makeSampleItem(int64(i), int64(i+1))
	}
	orig := makeCollector(6, items)
	origWeights := extractWeights(orig)

	_ = orig.PruneTo(2)

	// Original must be unchanged.
	require.Len(t, orig.Samples, 6)
	require.Equal(t, origWeights, extractWeights(orig))
}

// --- SamplePruner tests ---

func TestSamplePrunerFirstPartition(t *testing.T) {
	// First partition: no observations yet, should return budget/N.
	p := NewSamplePruner(10, 30000)
	count := p.PruneCount(1000000)
	require.Equal(t, 3000, count) // 30000/10 = 3000
}

func TestSamplePrunerFirstPartitionSmallBudget(t *testing.T) {
	// If budget/N < MinSampleFloor, clamp to MinSampleFloor.
	p := NewSamplePruner(100, 10000)
	count := p.PruneCount(1000)
	// 10000/100 = 100, but min floor is 500
	require.Equal(t, MinSampleFloor, count)
}

func TestSamplePrunerScalesByRowCount(t *testing.T) {
	p := NewSamplePruner(4, 4000) // base = 1000 per partition
	// Observe a first partition with 1000 rows.
	p.Observe(1000)

	// Second partition has 2000 rows (2x average) => scaled = 1000 * 2000/1000 = 2000
	count := p.PruneCount(2000)
	require.Equal(t, 2000, count)

	// Third partition has 500 rows (0.5x) => scaled = 1000 * 500/1000 = 500 = MinSampleFloor
	count = p.PruneCount(500)
	require.Equal(t, 500, count)
}

func TestSamplePrunerClampsToMinFloor(t *testing.T) {
	p := NewSamplePruner(4, 4000)
	p.Observe(10000) // avg = 10000

	// Tiny partition: 1 row => scaled = 1000 * 1/10000 = 0, clamped to 500
	count := p.PruneCount(1)
	require.Equal(t, MinSampleFloor, count)
}

func TestSamplePrunerClampsToBudget(t *testing.T) {
	p := NewSamplePruner(2, 2000)
	p.Observe(1) // avg = 1

	// Huge partition: scaled = 1000 * 1000000 / 1 = way over budget, clamped to 2000
	count := p.PruneCount(1000000)
	require.Equal(t, 2000, count)
}

func TestSamplePrunerSinglePartition(t *testing.T) {
	p := NewSamplePruner(1, 5000)
	count := p.PruneCount(100)
	require.Equal(t, 5000, count) // budget/1 = 5000
}

func TestSamplePrunerZeroPartitions(t *testing.T) {
	// Edge case: 0 partitions should not panic (max(0,1)=1).
	p := NewSamplePruner(0, 5000)
	count := p.PruneCount(100)
	require.Equal(t, 5000, count) // budget/1 = 5000
}

func TestSamplePrunerZeroRowsObserved(t *testing.T) {
	p := NewSamplePruner(4, 4000)
	// Observe a partition with 0 rows.
	p.Observe(0)
	// Next call: partitionsSoFar=1 but totalRowsSoFar=0, should use base path.
	count := p.PruneCount(1000)
	require.Equal(t, 1000, count) // base = 4000/4 = 1000
}

func TestSamplePrunerProgressiveAccuracy(t *testing.T) {
	// Verify that the running average converges.
	p := NewSamplePruner(5, 10000) // base = 2000
	// Process 5 partitions with rows: 1000, 2000, 3000, 4000, 5000
	rows := []int64{1000, 2000, 3000, 4000, 5000}
	for i, r := range rows {
		count := p.PruneCount(r)
		require.GreaterOrEqual(t, count, MinSampleFloor, "partition %d", i)
		p.Observe(r)
	}
	// After observing all 5, average = 3000.
	// A partition with 3000 rows should get exactly base (2000).
	count := p.PruneCount(3000)
	require.Equal(t, 2000, count)
}

func TestSamplePrunerObserveAccumulates(t *testing.T) {
	p := NewSamplePruner(3, 9000) // base = 3000
	p.Observe(100)
	p.Observe(200)
	// avg = 150, partition with 300 rows => scaled = 3000 * 300/150 = 6000
	count := p.PruneCount(300)
	require.Equal(t, 6000, count)
}

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
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 10; i++ {
		d := types.NewIntDatum(int64(2))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 7; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 5; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 3; i++ {
		d := types.NewIntDatum(int64(11))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
	}
	collector := &SampleCollector{
		Samples:   data,
		NullCount: 0,
		Count:     int64(len(data)),
		FMSketch:  sketch,
		TotalSize: int64(len(data)) * 8,
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	hist, topN, err := BuildHistAndTopN(ctx, 5, 4, 1, collector, tp, true, nil, false)
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
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 31; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 25; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(t, err)
		data = append(data, &SampleItem{Value: &d})
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
	_, topN, err := BuildHistAndTopN(ctx, 0, 100, 1, collector, tp, true, nil, false)
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
