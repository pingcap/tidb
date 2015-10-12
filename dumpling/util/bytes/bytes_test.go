package bytes

import (
	"fmt"

	. "github.com/pingcap/check"
)

var _ = Suite(&testBytesHelperSuites{})

type testBytesHelperSuites struct{}

func (s *testBytesHelperSuites) TestBytesClone(c *C) {
	b := []byte("hello world")
	shadowB := b
	newB := CloneBytes(b)
	c.Assert(b, DeepEquals, newB)
	c.Assert(fmt.Sprintf("%p", b) != fmt.Sprintf("%p", newB), IsTrue)
	c.Assert(fmt.Sprintf("%p", b), Equals, fmt.Sprintf("%p", shadowB))
}
