package memusage

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSuite{})

type testSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testSuite) TestRecord(c *C) {
	r := &Record{}
	r.Add(22)
	r.Add(37)
	r.Add(68)
	c.Assert(r.Allocated(), Equals, int64(127))

	r1 := r.Open()
	r1.Add(34)
	r1.Add(43)
	c.Assert(r1.Allocated(), Equals, int64(77))
	c.Assert(r.Allocated(), Equals, int64(204))

	r2 := r.Open()
	r2.Add(7)
	c.Assert(r2.Allocated(), Equals, int64(7))
	c.Assert(r.Allocated(), Equals, int64(211))

	r3 := r2.Open()
	r3.Add(8)
	c.Assert(r2.Allocated(), Equals, int64(15))
	c.Assert(r.Allocated(), Equals, int64(219))

	r1.Close()
	c.Assert(r.Allocated(), Equals, int64(142))

	r2.Close()
	c.Assert(r.Allocated(), Equals, int64(127))

	r.Close()
	c.Assert(r.Allocated(), Equals, int64(0))
}
