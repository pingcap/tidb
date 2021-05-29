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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
)

func (s *testStatisticsSuite) TestNewHistogramBySelectivity(c *C) {
	coll := &HistColl{
		Count:   330,
		Columns: make(map[int64]*Column),
		Indices: make(map[int64]*Index),
	}
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	intCol := &Column{}
	intCol.Histogram = *NewHistogram(1, 30, 30, 0, types.NewFieldType(mysql.TypeLonglong), chunk.InitialCapacity, 0)
	intCol.IsHandle = true
	for i := 0; i < 10; i++ {
		intCol.Bounds.AppendInt64(0, int64(i*3))
		intCol.Bounds.AppendInt64(0, int64(i*3+2))
		intCol.Buckets = append(intCol.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	coll.Columns[1] = intCol
	node := &StatsNode{ID: 1, Tp: PkType, Selectivity: 0.56}
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(nil), HighVal: types.MakeDatums(nil)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: types.MakeDatums(2)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(5), HighVal: types.MakeDatums(6)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(8), HighVal: types.MakeDatums(10)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(13), HighVal: types.MakeDatums(13)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(25), HighVal: []types.Datum{types.MaxValueDatum()}})
	intColResult := `column:1 ndv:16 totColSize:0
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10 ndv: 0
num: 11 lower_bound: 6 upper_bound: 8 repeats: 0 ndv: 0
num: 30 lower_bound: 9 upper_bound: 11 repeats: 0 ndv: 0
num: 1 lower_bound: 12 upper_bound: 14 repeats: 0 ndv: 0
num: 30 lower_bound: 27 upper_bound: 29 repeats: 0 ndv: 0`

	stringCol := &Column{}
	stringCol.Histogram = *NewHistogram(2, 15, 30, 0, types.NewFieldType(mysql.TypeString), chunk.InitialCapacity, 0)
	stringCol.Bounds.AppendString(0, "a")
	stringCol.Bounds.AppendString(0, "aaaabbbb")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 60})
	stringCol.Bounds.AppendString(0, "bbbb")
	stringCol.Bounds.AppendString(0, "fdsfdsfds")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 120})
	stringCol.Bounds.AppendString(0, "kkkkk")
	stringCol.Bounds.AppendString(0, "ooooo")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 180})
	stringCol.Bounds.AppendString(0, "oooooo")
	stringCol.Bounds.AppendString(0, "sssss")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 240})
	stringCol.Bounds.AppendString(0, "ssssssu")
	stringCol.Bounds.AppendString(0, "yyyyy")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 300})
	stringCol.PreCalculateScalar()
	coll.Columns[2] = stringCol
	node2 := &StatsNode{ID: 2, Tp: ColType, Selectivity: 0.6}
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums(nil), HighVal: types.MakeDatums(nil)})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: types.MakeDatums("aaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("aaaaaaaaaaa"), HighVal: types.MakeDatums("aaaaaaaaaaaaaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("bbb"), HighVal: types.MakeDatums("cccc")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ddd"), HighVal: types.MakeDatums("fff")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ggg"), HighVal: []types.Datum{types.MaxValueDatum()}})
	stringColResult := `column:2 ndv:9 totColSize:0
num: 60 lower_bound: a upper_bound: aaaabbbb repeats: 0 ndv: 0
num: 52 lower_bound: bbbb upper_bound: fdsfdsfds repeats: 0 ndv: 0
num: 54 lower_bound: kkkkk upper_bound: ooooo repeats: 0 ndv: 0
num: 60 lower_bound: oooooo upper_bound: sssss repeats: 0 ndv: 0
num: 60 lower_bound: ssssssu upper_bound: yyyyy repeats: 0 ndv: 0`

	newColl := coll.NewHistCollBySelectivity(sc, []*StatsNode{node, node2})
	c.Assert(newColl.Columns[1].String(), Equals, intColResult)
	c.Assert(newColl.Columns[2].String(), Equals, stringColResult)

	idx := &Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Name: model.NewCIStr("a"), Offset: 0}}}}
	coll.Indices[0] = idx
	idx.Histogram = *NewHistogram(0, 15, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	for i := 0; i < 5; i++ {
		low, err1 := codec.EncodeKey(sc, nil, types.NewIntDatum(int64(i*3)))
		c.Assert(err1, IsNil, Commentf("Test failed: %v", err1))
		high, err2 := codec.EncodeKey(sc, nil, types.NewIntDatum(int64(i*3+2)))
		c.Assert(err2, IsNil, Commentf("Test failed: %v", err2))
		idx.Bounds.AppendBytes(0, low)
		idx.Bounds.AppendBytes(0, high)
		idx.Buckets = append(idx.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	idx.PreCalculateScalar()
	node3 := &StatsNode{ID: 0, Tp: IndexType, Selectivity: 0.47}
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeDatums(2), HighVal: types.MakeDatums(3)})
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeDatums(10), HighVal: types.MakeDatums(13)})

	idxResult := `index:0 ndv:7
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10 ndv: 0
num: 30 lower_bound: 3 upper_bound: 5 repeats: 10 ndv: 0
num: 30 lower_bound: 9 upper_bound: 11 repeats: 10 ndv: 0
num: 30 lower_bound: 12 upper_bound: 14 repeats: 10 ndv: 0`

	newColl = coll.NewHistCollBySelectivity(sc, []*StatsNode{node3})
	c.Assert(newColl.Indices[0].String(), Equals, idxResult)
}

func (s *testStatisticsSuite) TestTruncateHistogram(c *C) {
	hist := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 1, 0)
	low, high := types.NewIntDatum(0), types.NewIntDatum(1)
	hist.AppendBucket(&low, &high, 0, 1)
	newHist := hist.TruncateHistogram(1)
	c.Assert(HistogramEqual(hist, newHist, true), IsTrue)
	newHist = hist.TruncateHistogram(0)
	c.Assert(newHist.Len(), Equals, 0)
}

func (s *testStatisticsSuite) TestValueToString4InvalidKey(c *C) {
	bytes, err := codec.EncodeKey(nil, nil, types.NewDatum(1), types.NewDatum(0.5))
	c.Assert(err, IsNil)
	// Append invalid flag.
	bytes = append(bytes, 20)
	datum := types.NewDatum(bytes)
	res, err := ValueToString(nil, &datum, 3, nil)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, "(1, 0.5, \x14)")
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

func genHist4Test(c *C, buckets []*bucket4Test, totColSize int64) *Histogram {
	h := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeBlob), len(buckets), totColSize)
	for _, bucket := range buckets {
		lower, err := codec.EncodeKey(nil, nil, types.NewIntDatum(bucket.lower))
		c.Assert(err, IsNil)
		upper, err := codec.EncodeKey(nil, nil, types.NewIntDatum(bucket.upper))
		c.Assert(err, IsNil)
		di, du := types.NewBytesDatum(lower), types.NewBytesDatum(upper)
		h.AppendBucketWithNDV(&di, &du, bucket.count, bucket.repeat, bucket.ndv)
	}
	return h
}

func (s *testStatisticsSuite) TestMergePartitionLevelHist(c *C) {
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

	for _, t := range tests {
		var expTotColSize int64
		hists := make([]*Histogram, 0, len(t.partitionHists))
		for i := range t.partitionHists {
			hists = append(hists, genHist4Test(c, t.partitionHists[i], t.totColSize[i]))
			expTotColSize += t.totColSize[i]
		}
		ctx := mock.NewContext()
		sc := ctx.GetSessionVars().StmtCtx
		poped := make([]TopNMeta, 0, len(t.popedTopN))
		for _, top := range t.popedTopN {
			b, err := codec.EncodeKey(sc, nil, types.NewIntDatum(top.data))
			c.Assert(err, IsNil)
			tmp := TopNMeta{
				Encoded: b,
				Count:   uint64(top.count),
			}
			poped = append(poped, tmp)
		}
		globalHist, err := MergePartitionHist2GlobalHist(sc, hists, poped, t.expBucketNumber, true)
		c.Assert(err, IsNil)
		for i, b := range t.expHist {
			lo, err := ValueToString(ctx.GetSessionVars(), globalHist.GetLower(i), 1, []byte{types.KindInt64})
			c.Assert(err, IsNil)
			up, err := ValueToString(ctx.GetSessionVars(), globalHist.GetUpper(i), 1, []byte{types.KindInt64})
			c.Assert(err, IsNil)
			c.Assert(fmt.Sprintf("%v", b.lower), Equals, lo)
			c.Assert(fmt.Sprintf("%v", b.upper), Equals, up)
			c.Assert(b.count, Equals, globalHist.Buckets[i].Count)
			c.Assert(b.repeat, Equals, globalHist.Buckets[i].Repeat)
			c.Assert(b.ndv, Equals, globalHist.Buckets[i].NDV)
		}
		c.Assert(globalHist.TotColSize, Equals, expTotColSize)
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

func (s *testStatisticsSuite) TestMergeBucketNDV(c *C) {
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
	for _, t := range tests {
		res, err := mergeBucketNDV(sc, &t.left, &t.right)
		c.Assert(err, IsNil)
		c.Assert(t.result.lower.GetInt64(), Equals, res.lower.GetInt64())
		c.Assert(t.result.upper.GetInt64(), Equals, res.upper.GetInt64())
		c.Assert(t.result.NDV, Equals, res.NDV)
		c.Assert(t.result.disjointNDV, Equals, res.disjointNDV)
	}
}
