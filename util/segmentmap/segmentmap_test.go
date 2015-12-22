package segmentmap

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testSegmentMapSuite{})

type testSegmentMapSuite struct {
}

func (s *testSegmentMapSuite) TestSegment(c *C) {
	segs := 2
	m := NewSegmentMap(segs)
	c.Assert(m.SegmentCount(), Equals, segs)
	k := []byte("k")
	v := []byte("v")
	val, exist := m.Get(k)
	c.Assert(exist, Equals, false)

	exist = m.Set(k, v, false)
	c.Assert(exist, IsFalse)

	val, exist = m.Get(k)
	c.Assert(v, DeepEquals, val.([]byte))
	c.Assert(exist, Equals, true)

	m0, err := m.GetSegment(0)
	c.Assert(err, IsNil)

	m1, err := m.GetSegment(1)
	c.Assert(err, IsNil)

	c.Assert(len(m0)+len(m1), Equals, 1)

	_, err = m.GetSegment(3)
	c.Assert(err, NotNil)
}
