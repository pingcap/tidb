// Copyright 2018 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTruncateHistogram(t *testing.T) {
	hist := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 1, 0)
	low, high := types.NewIntDatum(0), types.NewIntDatum(1)
	hist.AppendBucket(&low, &high, 0, 1)
	newHist := hist.TruncateHistogram(1)
	require.True(t, HistogramEqual(hist, newHist, true))
	newHist = hist.TruncateHistogram(0)
	require.Equal(t, 0, newHist.Len())
}

func TestValueToString4InvalidKey(t *testing.T) {
	bytes, err := codec.EncodeKey(nil, nil, types.NewDatum(1), types.NewDatum(0.5))
	require.NoError(t, err)
	// Append invalid flag.
	bytes = append(bytes, 20)
	datum := types.NewDatum(bytes)
	res, err := ValueToString(nil, &datum, 3, nil)
	require.NoError(t, err)
	require.Equal(t, "(1, 0.5, \x14)", res)
}

type bucket4Test struct {
	lower  int64
	upper  int64
	count  int64
	repeat int64
	ndv    int64
}

type topN4Test struct {
	data  int64
	count int64
}

func genHist4Test(t *testing.T, buckets []*bucket4Test, totColSize int64) *Histogram {
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

func TestMergePartitionLevelHist(t *testing.T) {
	type testCase struct {
		partitionHists  [][]*bucket4Test
		totColSize      []int64
		popedTopN       []topN4Test
		expHist         []*bucket4Test
		expBucketNumber int64
	}
	tests := []testCase{
		{
			partitionHists: [][]*bucket4Test{
				{
					// Col(1) = [1, 4,|| 6, 9, 9,|| 12, 12, 12,|| 13, 14, 15]
					{
						lower:  1,
						upper:  4,
						count:  2,
						repeat: 1,
						ndv:    2,
					},
					{
						lower:  6,
						upper:  9,
						count:  5,
						repeat: 2,
						ndv:    2,
					},
					{
						lower:  12,
						upper:  12,
						count:  8,
						repeat: 3,
						ndv:    1,
					},
					{
						lower:  13,
						upper:  15,
						count:  11,
						repeat: 1,
						ndv:    3,
					},
				},
				// Col(2) = [2, 5,|| 6, 7, 7,|| 11, 11, 11,|| 13, 14, 17]
				{
					{
						lower:  2,
						upper:  5,
						count:  2,
						repeat: 1,
						ndv:    2,
					},
					{
						lower:  6,
						upper:  7,
						count:  5,
						repeat: 2,
						ndv:    2,
					},
					{
						lower:  11,
						upper:  11,
						count:  8,
						repeat: 3,
						ndv:    1,
					},
					{
						lower:  13,
						upper:  17,
						count:  11,
						repeat: 1,
						ndv:    3,
					},
				},
			},
			totColSize: []int64{11, 11},
			popedTopN:  []topN4Test{},
			expHist: []*bucket4Test{
				{
					lower:  1,
					upper:  7,
					count:  7,
					repeat: 3,
					ndv:    5,
				},
				{
					lower:  7,
					upper:  11,
					count:  13,
					repeat: 3,
					ndv:    3,
				},
				{
					lower:  11,
					upper:  17,
					count:  22,
					repeat: 1,
					ndv:    6,
				},
			},
			expBucketNumber: 3,
		},
		{
			partitionHists: [][]*bucket4Test{
				{
					// Col(1) = [1, 4,|| 6, 9, 9,|| 12, 12, 12,|| 13, 14, 15]
					{
						lower:  1,
						upper:  4,
						count:  2,
						repeat: 1,
						ndv:    2,
					},
					{
						lower:  6,
						upper:  9,
						count:  5,
						repeat: 2,
						ndv:    2,
					},
					{
						lower:  12,
						upper:  12,
						count:  8,
						repeat: 3,
						ndv:    1,
					},
					{
						lower:  13,
						upper:  15,
						count:  11,
						repeat: 1,
						ndv:    3,
					},
				},
				// Col(2) = [2, 5,|| 6, 7, 7,|| 11, 11, 11,|| 13, 14, 17]
				{
					{
						lower:  2,
						upper:  5,
						count:  2,
						repeat: 1,
						ndv:    2,
					},
					{
						lower:  6,
						upper:  7,
						count:  5,
						repeat: 2,
						ndv:    2,
					},
					{
						lower:  11,
						upper:  11,
						count:  8,
						repeat: 3,
						ndv:    1,
					},
					{
						lower:  13,
						upper:  17,
						count:  11,
						repeat: 1,
						ndv:    3,
					},
				},
			},
			totColSize: []int64{11, 11},
			popedTopN: []topN4Test{
				{
					data:  18,
					count: 5,
				},
				{
					data:  4,
					count: 6,
				},
			},
			expHist: []*bucket4Test{
				{
					lower:  1,
					upper:  5,
					count:  10,
					repeat: 1,
					ndv:    3,
				},
				{
					lower:  5,
					upper:  12,
					count:  22,
					repeat: 3,
					ndv:    6,
				},
				{
					lower:  12,
					upper:  18,
					count:  33,
					repeat: 5,
					ndv:    6,
				},
			},
			expBucketNumber: 3,
		},
	}

	for _, tt := range tests {
		var expTotColSize int64
		hists := make([]*Histogram, 0, len(tt.partitionHists))
		for i := range tt.partitionHists {
			hists = append(hists, genHist4Test(t, tt.partitionHists[i], tt.totColSize[i]))
			expTotColSize += tt.totColSize[i]
		}
		ctx := mock.NewContext()
		sc := ctx.GetSessionVars().StmtCtx
		poped := make([]TopNMeta, 0, len(tt.popedTopN))
		for _, top := range tt.popedTopN {
			b, err := codec.EncodeKey(sc, nil, types.NewIntDatum(top.data))
			require.NoError(t, err)
			tmp := TopNMeta{
				Encoded: b,
				Count:   uint64(top.count),
			}
			poped = append(poped, tmp)
		}
		globalHist, err := MergePartitionHist2GlobalHist(sc, hists, poped, tt.expBucketNumber, true)
		require.NoError(t, err)
		for i, b := range tt.expHist {
			lo, err := ValueToString(ctx.GetSessionVars(), globalHist.GetLower(i), 1, []byte{types.KindInt64})
			require.NoError(t, err)
			up, err := ValueToString(ctx.GetSessionVars(), globalHist.GetUpper(i), 1, []byte{types.KindInt64})
			require.NoError(t, err)
			require.Equal(t, lo, fmt.Sprintf("%v", b.lower))
			require.Equal(t, up, fmt.Sprintf("%v", b.upper))
			require.Equal(t, globalHist.Buckets[i].Count, b.count)
			require.Equal(t, globalHist.Buckets[i].Repeat, b.repeat)
			require.Equal(t, globalHist.Buckets[i].NDV, b.ndv)
		}
		require.Equal(t, expTotColSize, globalHist.TotColSize)
	}
}

func genBucket4Merging4Test(lower, upper, ndv, disjointNDV int64) bucket4Merging {
	l := types.NewIntDatum(lower)
	r := types.NewIntDatum(upper)
	return bucket4Merging{
		lower: &l,
		upper: &r,
		Bucket: Bucket{
			NDV: ndv,
		},
		disjointNDV: disjointNDV,
	}
}

func TestMergeBucketNDV(t *testing.T) {
	type testData struct {
		left   bucket4Merging
		right  bucket4Merging
		result bucket4Merging
	}
	tests := []testData{
		{
			left:   genBucket4Merging4Test(1, 2, 2, 0),
			right:  genBucket4Merging4Test(1, 2, 3, 0),
			result: genBucket4Merging4Test(1, 2, 3, 0),
		},
		{
			left:   genBucket4Merging4Test(1, 3, 2, 0),
			right:  genBucket4Merging4Test(2, 3, 2, 0),
			result: genBucket4Merging4Test(1, 3, 3, 0),
		},
		{
			left:   genBucket4Merging4Test(1, 3, 2, 0),
			right:  genBucket4Merging4Test(4, 6, 2, 2),
			result: genBucket4Merging4Test(1, 3, 2, 4),
		},
		{
			left:   genBucket4Merging4Test(1, 5, 5, 0),
			right:  genBucket4Merging4Test(2, 6, 5, 0),
			result: genBucket4Merging4Test(1, 6, 6, 0),
		},
		{
			left:   genBucket4Merging4Test(3, 5, 3, 0),
			right:  genBucket4Merging4Test(2, 6, 4, 0),
			result: genBucket4Merging4Test(2, 6, 5, 0),
		},
	}
	sc := mock.NewContext().GetSessionVars().StmtCtx
	for _, tt := range tests {
		res, err := mergeBucketNDV(sc, &tt.left, &tt.right)
		require.NoError(t, err)
		require.Equal(t, res.lower.GetInt64(), tt.result.lower.GetInt64())
		require.Equal(t, res.upper.GetInt64(), tt.result.upper.GetInt64())
		require.Equal(t, res.NDV, tt.result.NDV)
		require.Equal(t, res.disjointNDV, tt.result.disjointNDV)
	}
}

func TestIndexQueryBytes(t *testing.T) {
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	idx := &Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Name: model.NewCIStr("a"), Offset: 0}}}}
	idx.Histogram = *NewHistogram(0, 15, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	low, err1 := codec.EncodeKey(sc, nil, types.NewBytesDatum([]byte("0")))
	require.NoError(t, err1)
	high, err2 := codec.EncodeKey(sc, nil, types.NewBytesDatum([]byte("3")))
	require.NoError(t, err2)
	idx.Bounds.AppendBytes(0, low)
	idx.Bounds.AppendBytes(0, high)
	idx.Buckets = append(idx.Buckets, Bucket{Repeat: 10, Count: 20, NDV: 20})
	idx.PreCalculateScalar()
	idx.CMSketch = nil
	// Count / NDV
	require.Equal(t, idx.QueryBytes(nil, low), uint64(1))
	// Repeat
	require.Equal(t, idx.QueryBytes(nil, high), uint64(10))
}
