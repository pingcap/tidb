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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

const partition = 1000
const histogramLen = 100
const popedTopNLen = 100

var magicLower = [histogramLen]int64{
	0, 100, 200, 300, 400, 500, 600, 700, 800, 900,
	1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900,
	2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900,
	3000, 3100, 3200, 3300, 3400, 3500, 3600, 3700, 3800, 3900,
	4000, 4100, 4200, 4300, 4400, 4500, 4600, 4700, 4800, 4900,
	5000, 5100, 5200, 5300, 5400, 5500, 5600, 5700, 5800, 5900,
	6000, 6100, 6200, 6300, 6400, 6500, 6600, 6700, 6800, 6900,
	7000, 7100, 7200, 7300, 7400, 7500, 7600, 7700, 7800, 7900,
	8000, 8100, 8200, 8300, 8400, 8500, 8600, 8700, 8800, 8900,
	9000, 9100, 9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900,
}

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
			upper = lower + (rand.Int63n(magicLower[n+1] - lower))
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
		lower, err := codec.EncodeKey(nil, nil, types.NewIntDatum(bucket.lower))
		require.NoError(t, err)
		upper, err := codec.EncodeKey(nil, nil, types.NewIntDatum(bucket.upper))
		require.NoError(t, err)
		di, du := types.NewBytesDatum(lower), types.NewBytesDatum(upper)
		h.AppendBucketWithNDV(&di, &du, bucket.count, bucket.repeat, bucket.ndv)
	}
	return h
}

// cmd: go test -run=^$ -bench=BenchmarkMergePartitionHist2GlobalHist -benchmem github.com/pingcap/tidb/statistics
func BenchmarkMergePartitionHist2GlobalHist(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctx := mock.NewContext()
		sc := ctx.GetSessionVars().StmtCtx
		hists := make([]*Histogram, 0, partition)
		for i := 0; i < partition; i++ {
			buckets := genBucket4TestData(histogramLen)
			hist := genHist4Bench(b, buckets, histogramLen)
			hists = append(hists, hist)
		}
		const expBucketNumber = 100
		poped := make([]TopNMeta, 0, popedTopNLen)
		for n := 0; n < popedTopNLen; n++ {
			b, _ := codec.EncodeKey(sc, nil, types.NewIntDatum(rand.Int63n(10000)))
			tmp := TopNMeta{
				Encoded: b,
				Count:   uint64(rand.Int63n(10000)),
			}
			poped = append(poped, tmp)
		}
		b.StartTimer()
		MergePartitionHist2GlobalHist(sc, hists, poped, expBucketNumber, true)
	}
}
