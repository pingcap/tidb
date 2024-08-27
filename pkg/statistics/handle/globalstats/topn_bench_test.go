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

package globalstats

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"github.com/tiancaiamao/gp"
)

func prepareTopNsAndHists(b *testing.B, partitions int, tz *time.Location) ([]*statistics.TopN, []*statistics.Histogram) {
	sc := stmtctx.NewStmtCtxWithTimeZone(tz)
	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct TopN, should be key1 -> rand(0, 1000), key2 -> rand(0, 1000), key3 -> rand(0, 1000)...
		topN := statistics.NewTopN(500)
		{
			for j := 1; j <= 500; j++ {
				// Randomly skip some keys for some partitions.
				if i%2 == 0 && j%2 == 0 {
					continue
				}
				key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(int64(j)))
				require.NoError(b, err)
				topN.AppendTopN(key, uint64(rand.Intn(1000)))
			}
		}
		topNs = append(topNs, topN)
	}

	// Prepare Hists.
	hists := make([]*statistics.Histogram, 0, partitions)
	for i := 0; i < partitions; i++ {
		// Construct Hist
		h := statistics.NewHistogram(1, 500, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		for j := 1; j <= 500; j++ {
			datum := types.NewIntDatum(int64(j))
			h.AppendBucket(&datum, &datum, int64(10+j*10), 10)
		}
		hists = append(hists, h)
	}

	return topNs, hists
}

func benchmarkMergePartTopN2GlobalTopNWithHists(partitions int, b *testing.B) {
	loc := time.UTC
	version := 1
	killer := sqlkiller.SQLKiller{}
	topNs, hists := prepareTopNsAndHists(b, partitions, loc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Benchmark merge 100 topN.
		_, _, _, _ = MergePartTopN2GlobalTopN(
			loc,
			version,
			topNs,
			100,
			hists,
			false,
			&killer,
		)
	}
}

var benchmarkSizes = []int{100, 1000, 2000, 5000, 10000}

// cmd: go test -run=^$ -bench=BenchmarkMergePartTopN2GlobalTopNWithHists -benchmem github.com/pingcap/tidb/pkg/statistics/handle/globalstats
func BenchmarkMergePartTopN2GlobalTopNWithHists(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkMergePartTopN2GlobalTopNWithHists(size, b)
		})
	}
}

func benchmarkMergeGlobalStatsTopNByConcurrencyWithHists(partitions int, b *testing.B) {
	loc := time.UTC
	version := 1
	killer := sqlkiller.SQLKiller{}

	topNs, hists := prepareTopNsAndHists(b, partitions, loc)
	wrapper := NewStatsWrapper(hists, topNs)
	const mergeConcurrency = 4
	batchSize := len(wrapper.AllTopN) / mergeConcurrency
	if batchSize < 1 {
		batchSize = 1
	} else if batchSize > MaxPartitionMergeBatchSize {
		batchSize = MaxPartitionMergeBatchSize
	}
	gpool := gp.New(mergeConcurrency, 5*time.Minute)
	defer gpool.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Benchmark merge 100 topN.
		_, _, _, _ = MergeGlobalStatsTopNByConcurrency(
			gpool,
			mergeConcurrency,
			batchSize,
			wrapper,
			loc,
			version,
			100,
			false,
			&killer,
		)
	}
}

// cmd: go test -run=^$ -bench=BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists -benchmem github.com/pingcap/tidb/pkg/statistics/handle/globalstats
func BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkMergeGlobalStatsTopNByConcurrencyWithHists(size, b)
		})
	}
}
