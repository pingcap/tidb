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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
)

var _ = Suite(&HistogramTestSuite{})

type HistogramTestSuite struct {
}

func (s *testStatisticsSuite) TestNewHistogramBySelectivity(c *C) {
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	intCol := &Column{}
	intCol.Histogram = *NewHistogram(1, 30, 0, 0, types.NewFieldType(mysql.TypeLonglong), chunk.InitialCapacity, 0)
	for i := 0; i < 10; i++ {
		intCol.Bounds.AppendInt64(0, int64(i*3))
		intCol.Bounds.AppendInt64(0, int64(i*3+2))
		intCol.Buckets = append(intCol.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	node := &StatsNode{ID: 1, Tp: pkType, Selectivity: 0.2}
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(5), HighVal: types.MakeDatums(6)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(8), HighVal: types.MakeDatums(10)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(13), HighVal: types.MakeDatums(13)})
	newIntCol, err := intCol.newNumericColumnBySelectivity(sc, node)
	c.Assert(err, IsNil, Commentf("Test failed: %v", err))
	intColResult := `column:1 ndv:6 totColSize:0
num: 10 lower_bound: 5 upper_bound: 5 repeats: 10
num: 20 lower_bound: 6 upper_bound: 8 repeats: 10
num: 20 lower_bound: 9 upper_bound: 11 repeats: 10
num: 10 lower_bound: 13 upper_bound: 14 repeats: 10`
	c.Assert(newIntCol.String(), Equals, intColResult)

	stringCol := &Column{}
	stringCol.Histogram = *NewHistogram(2, 9, 0, 0, types.NewFieldType(mysql.TypeString), chunk.InitialCapacity, 0)
	stringCol.Bounds.AppendString(0, "a")
	stringCol.Bounds.AppendString(0, "aaaabbbb")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 30})
	stringCol.Bounds.AppendString(0, "bbbb")
	stringCol.Bounds.AppendString(0, "fdsfdsfds")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 60})
	stringCol.Bounds.AppendString(0, "kkkkk")
	stringCol.Bounds.AppendString(0, "yyyyy")
	stringCol.Buckets = append(stringCol.Buckets, Bucket{Repeat: 10, Count: 90})
	node.Tp = colType
	node.ID = 2
	node.Ranges = node.Ranges[:0]
	node.Selectivity = 0.4
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(""), HighVal: types.MakeDatums("aaa")})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums("aaaaaaaaaaa"), HighVal: types.MakeDatums("aaaaaaaaaaaaaa")})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums("bbb"), HighVal: types.MakeDatums("cccc")})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums("ddd"), HighVal: types.MakeDatums("fff")})
	newStringCol, err := stringCol.newNonNumericColumnBySelectivity(sc, node)
	c.Assert(err, IsNil, Commentf("Test failed: %v", err))
	stringColResult := `column:2 ndv:3 totColSize:0
num: 30 lower_bound: a upper_bound: aaaabbbb repeats: 10
num: 30 lower_bound: bbbb upper_bound: fdsfdsfds repeats: 10`
	c.Assert(newStringCol.String(), Equals, stringColResult)
	idx := Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Name: model.NewCIStr("a"), Offset: 0}}}}
	idx.Histogram = *NewHistogram(0, 15, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	for i := 0; i < 5; i++ {
		low, err1 := codec.EncodeKey(sc, nil, types.NewIntDatum(int64(i*3)))
		c.Assert(err1, IsNil, Commentf("Test failed: %v", err))
		high, err2 := codec.EncodeKey(sc, nil, types.NewIntDatum(int64(i*3+2)))
		c.Assert(err2, IsNil, Commentf("Test failed: %v", err))
		idx.Bounds.AppendBytes(0, low)
		idx.Bounds.AppendBytes(0, high)
		idx.Buckets = append(idx.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	node.Tp = indexType
	node.ID = 0
	node.Selectivity = 0.47
	node.Ranges = node.Ranges[:0]
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(2), HighVal: types.MakeDatums(3)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeDatums(7), HighVal: types.MakeDatums(11)})
	newIdx, err := idx.newIndexBySelectivity(sc, node)
	c.Assert(err, IsNil, Commentf("Test failed: %v", err))
	idxResult := `index:0 ndv:7
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 30 lower_bound: 3 upper_bound: 5 repeats: 10
num: 30 lower_bound: 6 upper_bound: 8 repeats: 10
num: 30 lower_bound: 9 upper_bound: 11 repeats: 10`
	c.Assert(newIdx.String(), Equals, idxResult)
}
