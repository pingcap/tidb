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
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 11 lower_bound: 6 upper_bound: 8 repeats: 0
num: 30 lower_bound: 9 upper_bound: 11 repeats: 0
num: 1 lower_bound: 12 upper_bound: 14 repeats: 0
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
	node2 := &StatsNode{ID: 2, Tp: ColType, Selectivity: 0.6}
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums(nil), HighVal: types.MakeDatums(nil)})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: types.MakeDatums("aaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("aaaaaaaaaaa"), HighVal: types.MakeDatums("aaaaaaaaaaaaaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("bbb"), HighVal: types.MakeDatums("cccc")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ddd"), HighVal: types.MakeDatums("fff")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeDatums("ggg"), HighVal: []types.Datum{types.MaxValueDatum()}})
	stringColResult := `column:2 ndv:9 totColSize:0
num: 60 lower_bound: a upper_bound: aaaabbbb repeats: 0
num: 52 lower_bound: bbbb upper_bound: fdsfdsfds repeats: 0
num: 54 lower_bound: kkkkk upper_bound: ooooo repeats: 0
num: 60 lower_bound: oooooo upper_bound: sssss repeats: 0
num: 60 lower_bound: ssssssu upper_bound: yyyyy repeats: 0`

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
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 30 lower_bound: 3 upper_bound: 5 repeats: 10
num: 30 lower_bound: 9 upper_bound: 11 repeats: 10
num: 30 lower_bound: 12 upper_bound: 14 repeats: 10`

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
	res, err := ValueToString(&datum, 3, nil)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, "(1, 0.5, \x14)")
}
