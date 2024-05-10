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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

const (
	histogramLen    = 100
	popedTopNLen    = 100
	expBucketNumber = 100
)

func genBucket4TestData(length int) []*bucket4Test {
	result := make([]*bucket4Test, 0, length)
	var lower, upper int64
	for n := 0; n < length; n++ {
		if n == 0 {
			lower = 0
		} else {
			lower = upper + 1
		}
		if n == length-1 {
			upper = 10000
		} else {
			upper = lower + (rand.Int63n(int64(100*(n+1)) - lower))
		}
		result = append(result, &bucket4Test{
			lower:  lower,
			upper:  upper,
			count:  rand.Int63n(10000),
			repeat: rand.Int63n(100),
			ndv:    rand.Int63n(100),
		})
	}
	return result
}

func genHist4Bench(t *testing.B, buckets []*bucket4Test, totColSize int64) *Histogram {
	h := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeBlob), len(buckets), totColSize)
	for _, bucket := range buckets {
		lower, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(bucket.lower))
		require.NoError(t, err)
		upper, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(bucket.upper))
		require.NoError(t, err)
		di, du := types.NewBytesDatum(lower), types.NewBytesDatum(upper)
		h.AppendBucketWithNDV(&di, &du, bucket.count, bucket.repeat, bucket.ndv)
	}
	return h
}

func benchmarkMergePartitionHist2GlobalHist(b *testing.B, partition int) {
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	hists := make([]*Histogram, 0, partition)
	for i := 0; i < partition; i++ {
		buckets := genBucket4TestData(histogramLen)
		hist := genHist4Bench(b, buckets, histogramLen)
		hists = append(hists, hist)
	}
	poped := make([]TopNMeta, 0, popedTopNLen)
	for n := 0; n < popedTopNLen; n++ {
		b, _ := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(rand.Int63n(10000)))
		tmp := TopNMeta{
			Encoded: b,
			Count:   uint64(rand.Int63n(10000)),
		}
		poped = append(poped, tmp)
	}
	b.StartTimer()
	MergePartitionHist2GlobalHist(sc, hists, poped, expBucketNumber, true)
	b.StopTimer()
}

var benchmarkPartitionSize = []int{1000, 10000, 100000}

// cmd: go test -run=^$ -bench=BenchmarkMergePartitionHist2GlobalHist -benchmem github.com/pingcap/tidb/pkg/statistics
func BenchmarkMergePartitionHist2GlobalHist(b *testing.B) {
	for _, size := range benchmarkPartitionSize {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkMergePartitionHist2GlobalHist(b, size)
			}
		})
	}
}
