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
	. "github.com/pingcap/check"
)

func (s *testStatisticsSuite) TestSketch(c *C) {
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(s.samples, maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(6624))

	rcSketch, ndv, err := buildFMSketch(s.rc.(*recordSet).data, maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(74240))

	pkSketch, ndv, err := buildFMSketch(s.pk.(*recordSet).data, maxSize)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(99968))

	var sketches []*FMSketch
	sketches = append(sketches, sampleSketch)
	sketches = append(sketches, pkSketch)
	sketches = append(sketches, rcSketch)
	_, ndv = mergeFMSketches(sketches, maxSize)
	c.Check(ndv, Equals, int64(99968))

	maxSize = 2
	sketch := NewFMSketch(maxSize)
	sketch.insertHashValue(1)
	sketch.insertHashValue(2)
	c.Check(len(sketch.hashset), Equals, maxSize)
	sketch.insertHashValue(4)
	c.Check(len(sketch.hashset), LessEqual, maxSize)
}
