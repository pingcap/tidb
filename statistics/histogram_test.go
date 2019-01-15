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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
)

var _ = Suite(&HistogramTestSuite{})

type HistogramTestSuite struct {
}

func (s *HistogramTestSuite) TestMergeHistogramForInnerJoinIntCase(c *C) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	// aHist: 60 distinct value, each value repeats 2 times.
	aHist := NewHistogram(0, 60, 0, 0, intTp, chunk.InitialCapacity, 0)
	// [100, 200]
	aHist.Bounds.AppendInt64(0, 100)
	aHist.Bounds.AppendInt64(0, 200)
	aHist.Buckets = append(aHist.Buckets, Bucket{Repeat: 2, Count: 100})
	// [210, 230]
	aHist.Bounds.AppendInt64(0, 210)
	aHist.Bounds.AppendInt64(0, 230)
	aHist.Buckets = append(aHist.Buckets, Bucket{Repeat: 2, Count: 120})
	// bHist: 100 distinct value, each value repeats 100 times.
	bHist := NewHistogram(0, 100, 0, 0, intTp, chunk.InitialCapacity, 0)
	// [90, 120]
	bHist.Bounds.AppendInt64(0, 90)
	bHist.Bounds.AppendInt64(0, 120)
	bHist.Buckets = append(bHist.Buckets, Bucket{Repeat: 100, Count: 3000})
	// [130, 160]
	bHist.Bounds.AppendInt64(0, 130)
	bHist.Bounds.AppendInt64(0, 160)
	bHist.Buckets = append(bHist.Buckets, Bucket{Repeat: 100, Count: 6000})
	// [180, 220]
	bHist.Bounds.AppendInt64(0, 180)
	bHist.Bounds.AppendInt64(0, 220)
	bHist.Buckets = append(bHist.Buckets, Bucket{Repeat: 100, Count: 10000})
	finalHist := MergeHistogramForInnerJoin(aHist, bHist, intTp)

	c.Assert(finalHist.ToString(0), Equals, `column:0 ndv:41 totColSize:0
num: 2079 lower_bound: 100 upper_bound: 120 repeats: 200
num: 3069 lower_bound: 130 upper_bound: 160 repeats: 200
num: 2079 lower_bound: 180 upper_bound: 200 repeats: 200
num: 1047 lower_bound: 210 upper_bound: 220 repeats: 200`)

}

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
	intCol.isHandle = true
	for i := 0; i < 10; i++ {
		intCol.Bounds.AppendInt64(0, int64(i*3))
		intCol.Bounds.AppendInt64(0, int64(i*3+2))
		intCol.Buckets = append(intCol.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	coll.Columns[1] = intCol
	node := &StatsNode{ID: 1, Tp: pkType, Selectivity: 0.56}
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(nil), HighVal: types.MakeDatums(nil)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: types.MakeDatums(2)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(5), HighVal: types.MakeDatums(6)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(8), HighVal: types.MakeDatums(10)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(13), HighVal: types.MakeDatums(13)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(25), HighVal: []types.Datum{types.MaxValueDatum()}})
	intColResult := `column:1 ndv:16 totColSize:0
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 10 lower_bound: 3 upper_bound: 5 repeats: 10
num: 20 lower_bound: 6 upper_bound: 8 repeats: 10
num: 20 lower_bound: 9 upper_bound: 11 repeats: 0
num: 10 lower_bound: 12 upper_bound: 14 repeats: 0
num: 20 lower_bound: 24 upper_bound: 26 repeats: 10
num: 30 lower_bound: 27 upper_bound: 29 repeats: 0`

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
	node2 := &StatsNode{ID: 2, Tp: colType, Selectivity: 0.6}
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums(nil), HighVal: types.MakeDatums(nil)})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: types.MakeDatums("aaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("aaaaaaaaaaa"), HighVal: types.MakeDatums("aaaaaaaaaaaaaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("bbb"), HighVal: types.MakeDatums("cccc")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ddd"), HighVal: types.MakeDatums("fff")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ggg"), HighVal: []types.Datum{types.MaxValueDatum()}})
	stringColResult := `column:2 ndv:9 totColSize:0
num: 60 lower_bound: a upper_bound: aaaabbbb repeats: 0
num: 60 lower_bound: bbbb upper_bound: fdsfdsfds repeats: 20
num: 60 lower_bound: kkkkk upper_bound: ooooo repeats: 20
num: 60 lower_bound: oooooo upper_bound: sssss repeats: 20
num: 60 lower_bound: ssssssu upper_bound: yyyyy repeats: 0`

	newColl := coll.NewHistCollBySelectivity(sc, []*StatsNode{node, node2})
	c.Assert(newColl.Columns[1].String(), Equals, intColResult)
	c.Assert(newColl.Columns[2].String(), Equals, stringColResult)

	idx := &Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Name: model.NewCIStr("a"), Offset: 0}}}}
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
	node3 := &StatsNode{ID: 0, Tp: indexType, Selectivity: 0.47}
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeDatums(2), HighVal: types.MakeDatums(3)})
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeDatums(10), HighVal: types.MakeDatums(13)})

	idxResult := `index:0 ndv:7
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 30 lower_bound: 3 upper_bound: 5 repeats: 10
num: 30 lower_bound: 9 upper_bound: 11 repeats: 10
num: 30 lower_bound: 12 upper_bound: 14 repeats: 10`

	newIdx, err := idx.newIndexBySelectivity(sc, node3)
	c.Assert(err, IsNil, Commentf("Test failed: %v", err))
	c.Assert(newIdx.String(), Equals, idxResult)
}
