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

package statistics_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tiancaiamao/gp"
)

// cmd: go test -run=^$ -bench=BenchmarkMergePartTopN2GlobalTopNWithHists -benchmem github.com/pingcap/tidb/statistics
func benchmarkMergePartTopN2GlobalTopNWithHists(partitions int, b *testing.B) {
	loc := time.UTC
	sc := &stmtctx.StatementContext{TimeZone: loc}
	version := 1
	isKilled := uint32(0)

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct TopN, should be key1 -> 2, key2 -> 2, key3 -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc, nil, types.NewIntDatum(1))
			require.NoError(b, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc, nil, types.NewIntDatum(2))
			require.NoError(b, err)
			topN.AppendTopN(key2, 2)
			if i%2 == 0 {
				key3, err := codec.EncodeKey(sc, nil, types.NewIntDatum(3))
				require.NoError(b, err)
				topN.AppendTopN(key3, 3)
			}
		}
		topNs = append(topNs, topN)
	}

	// Prepare Hists.
	hists := make([]*statistics.Histogram, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct Hist
		h := statistics.NewHistogram(1, 10, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		h.Bounds.AppendInt64(0, 1)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 20})
		h.Bounds.AppendInt64(0, 2)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 3)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 4)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 40})
		hists = append(hists, h)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Benchmark merge 10 topN.
		_, _, _, _ = statistics.MergePartTopN2GlobalTopN(loc, version, topNs, 10, hists, false, &isKilled)
	}
}

// cmd: go test -run=^$ -bench=BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists -benchmem github.com/pingcap/tidb/statistics
func benchmarkMergeGlobalStatsTopNByConcurrencyWithHists(partitions int, b *testing.B) {
	loc := time.UTC
	sc := &stmtctx.StatementContext{TimeZone: loc}
	version := 1
	isKilled := uint32(0)

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct TopN, should be key1 -> 2, key2 -> 2, key3 -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc, nil, types.NewIntDatum(1))
			require.NoError(b, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc, nil, types.NewIntDatum(2))
			require.NoError(b, err)
			topN.AppendTopN(key2, 2)
			if i%2 == 0 {
				key3, err := codec.EncodeKey(sc, nil, types.NewIntDatum(3))
				require.NoError(b, err)
				topN.AppendTopN(key3, 3)
			}
		}
		topNs = append(topNs, topN)
	}

	// Prepare Hists.
	hists := make([]*statistics.Histogram, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct Hist
		h := statistics.NewHistogram(1, 10, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		h.Bounds.AppendInt64(0, 1)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 20})
		h.Bounds.AppendInt64(0, 2)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 3)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 4)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 40})
		hists = append(hists, h)
	}
	wrapper := statistics.NewStatsWrapper(hists, topNs)
	const mergeConcurrency = 4
	batchSize := len(wrapper.AllTopN) / mergeConcurrency
	if batchSize < 1 {
		batchSize = 1
	} else if batchSize > handle.MaxPartitionMergeBatchSize {
		batchSize = handle.MaxPartitionMergeBatchSize
	}
	gpool := gp.New(mergeConcurrency, 5*time.Minute)
	defer gpool.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Benchmark merge 10 topN.
		_, _, _, _ = handle.MergeGlobalStatsTopNByConcurrency(gpool, mergeConcurrency, batchSize, wrapper, loc, version, 10, false, &isKilled)
	}
}

var benchmarkSizes = []int{100, 1000, 10000, 100000, 1000000, 10000000}
var benchmarkConcurrencySizes = []int{100, 1000, 10000, 100000}

func BenchmarkMergePartTopN2GlobalTopNWithHists(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkMergePartTopN2GlobalTopNWithHists(size, b)
		})
	}
}

func BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists(b *testing.B) {
	for _, size := range benchmarkConcurrencySizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkMergeGlobalStatsTopNByConcurrencyWithHists(size, b)
		})
	}
}
