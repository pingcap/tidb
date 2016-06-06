// Copyright 2015 PingCAP, Inc.
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

package segmentmap

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testSegmentMapSuite{})

type testSegmentMapSuite struct {
}

func (s *testSegmentMapSuite) TestSegment(c *C) {
	defer testleak.AfterTest(c)()
	segs := int64(2)
	m, err := NewSegmentMap(segs)
	c.Assert(err, IsNil)
	c.Assert(m.SegmentCount(), Equals, segs)
	k := []byte("k")
	v := []byte("v")
	val, exist := m.Get(k)
	c.Assert(exist, IsFalse)

	exist = m.Set(k, v, false)
	c.Assert(exist, IsFalse)

	val, exist = m.Get(k)
	c.Assert(v, DeepEquals, val.([]byte))
	c.Assert(exist, IsTrue)

	m0, err := m.GetSegment(0)
	c.Assert(err, IsNil)

	m1, err := m.GetSegment(1)
	c.Assert(err, IsNil)

	c.Assert(len(m0)+len(m1), Equals, 1)

	_, err = m.GetSegment(3)
	c.Assert(err, NotNil)
}
