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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
