// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// BenchmarkBuildHistAndTopN is used to benchmark the performance of BuildHistAndTopN.
// go test -benchmem -run=^$ -bench ^BenchmarkBuildHistAndTopN$ github.com/pingcap/tidb/pkg/statistics
// * The NDV is 1000000
func BenchmarkBuildHistAndTopN(b *testing.B) {
	ctx := mock.NewContext()
	const cnt = 1000_000
	sketch := NewFMSketch(cnt)
	data := make([]*SampleItem, 0, 8)
	for i := 1; i <= cnt; i++ {
		d := types.NewIntDatum(int64(i))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 10; i++ {
		d := types.NewIntDatum(int64(2))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 7; i++ {
		d := types.NewIntDatum(int64(4))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 5; i++ {
		d := types.NewIntDatum(int64(7))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: &d})
	}
	for i := 1; i < 3; i++ {
		d := types.NewIntDatum(int64(11))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: &d})
	}
	collector := &SampleCollector{
		Samples:   data,
		NullCount: 0,
		Count:     int64(len(data)),
		FMSketch:  sketch,
		TotalSize: int64(len(data)) * 8,
	}
	filedType := types.NewFieldType(mysql.TypeLong)
	memoryTracker := memory.NewTracker(10, 1024*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = BuildHistAndTopN(ctx, 256, 500, 0, collector, filedType, true, memoryTracker, false)
	}
}

// BenchmarkBuildHistAndTopNWithLowNDV is used to benchmark the performance of BuildHistAndTopN with low NDV.
// go test -benchmem -run=^$ -bench ^BenchmarkBuildHistAndTopNWithLowNDV github.com/pingcap/tidb/pkg/statistics
// * NDV is 102
func BenchmarkBuildHistAndTopNWithLowNDV(b *testing.B) {
	ctx := mock.NewContext()
	const cnt = 1000_000
	sketch := NewFMSketch(cnt)
	data := make([]*SampleItem, 0, 8)
	total := 0
	for i := 1; i <= 1_000; i++ {
		total++
		d := types.NewIntDatum(int64(1000))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: d})
	}
	for i := 1; i <= 1_000; i++ {
		total++
		d := types.NewIntDatum(int64(2000))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: d})
	}
	end := total / 2
	for range end {
		total++
		d := types.NewIntDatum(rand.Int63n(50))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: d})
	}
	end = cnt - total
	for range end {
		d := types.NewIntDatum(rand.Int63n(100))
		err := sketch.InsertValue(ctx.GetSessionVars().StmtCtx, d)
		require.NoError(b, err)
		data = append(data, &SampleItem{Value: d})
	}
	collector := &SampleCollector{
		Samples:   data,
		NullCount: 0,
		Count:     int64(len(data)),
		FMSketch:  sketch,
		TotalSize: int64(len(data)) * 8,
	}
	filedType := types.NewFieldType(mysql.TypeLong)
	memoryTracker := memory.NewTracker(10, 1024*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = BuildHistAndTopN(ctx, 256, 500, 0, collector, filedType, true, memoryTracker, false)
	}
}

// SequentialRangeChecker tests
func TestSequentialRangeChecker(t *testing.T) {
	ranges := []TopNWithRange{
		{TopNMeta: TopNMeta{Count: 10}, startIdx: 5, endIdx: 8},   // range [5,8]
		{TopNMeta: TopNMeta{Count: 15}, startIdx: 12, endIdx: 15}, // range [12,15]
		{TopNMeta: TopNMeta{Count: 8}, startIdx: 20, endIdx: 22},  // range [20,22]
	}

	checker := NewSequentialRangeChecker(ranges)

	// test basic functionality
	require.False(t, checker.IsIndexInTopNRange(4))  // before first range
	require.True(t, checker.IsIndexInTopNRange(6))   // in first range
	require.False(t, checker.IsIndexInTopNRange(10)) // between ranges
	require.True(t, checker.IsIndexInTopNRange(13))  // in second range
	require.True(t, checker.IsIndexInTopNRange(21))  // in third range
	require.False(t, checker.IsIndexInTopNRange(25)) // after all ranges

	// test edge cases
	emptyChecker := NewSequentialRangeChecker([]TopNWithRange{})
	require.False(t, emptyChecker.IsIndexInTopNRange(0))

	// test unsorted input - should be sorted internally
	unsortedRanges := []TopNWithRange{
		{TopNMeta: TopNMeta{Count: 8}, startIdx: 20, endIdx: 22},  // third
		{TopNMeta: TopNMeta{Count: 10}, startIdx: 5, endIdx: 8},   // first
		{TopNMeta: TopNMeta{Count: 15}, startIdx: 12, endIdx: 15}, // second
	}
	unsortedChecker := NewSequentialRangeChecker(unsortedRanges)
	require.True(t, unsortedChecker.IsIndexInTopNRange(6))
	require.True(t, unsortedChecker.IsIndexInTopNRange(13))
	require.True(t, unsortedChecker.IsIndexInTopNRange(21))
}
