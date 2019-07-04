// Copyright 2017 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// extractSampleItemsDatums is for test purpose only to extract Datum slice
// from SampleItem slice.
func extractSampleItemsDatums(items []*SampleItem) []types.Datum {
	datums := make([]types.Datum, len(items))
	for i, item := range items {
		datums[i] = item.Value
	}
	return datums
}

func (s *testStatisticsSuite) TestSketch(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(6232))

	rcSketch, ndv, err := buildFMSketch(sc, s.rc.(*recordSet).data, maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(73344))

	pkSketch, ndv, err := buildFMSketch(sc, s.pk.(*recordSet).data, maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(100480))

	sampleSketch.mergeFMSketch(pkSketch)
	sampleSketch.mergeFMSketch(rcSketch)
	c.Check(sampleSketch.NDV(), Equals, int64(100480))

	maxSize = 2
	sketch := NewFMSketch(maxSize)
	sketch.insertHashValue(1)
	sketch.insertHashValue(2)
	c.Check(len(sketch.hashset), Equals, maxSize)
	sketch.insertHashValue(4)
	c.Check(len(sketch.hashset), LessEqual, maxSize)
}

func (s *testStatisticsSuite) TestSketchProtoConversion(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(6232))

	p := FMSketchToProto(sampleSketch)
	f := FMSketchFromProto(p)
	c.Assert(sampleSketch.mask, Equals, f.mask)
	c.Assert(len(sampleSketch.hashset), Equals, len(f.hashset))
	for val := range sampleSketch.hashset {
		c.Assert(f.hashset[val], IsTrue)
	}
}
