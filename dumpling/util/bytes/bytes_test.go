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
	// Ugly hacks.Go doesn't allow compare two slice (except nil).
	// For example:  b == newB <-- it's invalid.
	// In this test, we must ensure CloneBytes method returns a new slice with
	// the same value, so we need to check the new slice's address.
	c.Assert(fmt.Sprintf("%p", b) != fmt.Sprintf("%p", newB), IsTrue)
	// But the addresses are the same when it's a shadow copy.
	c.Assert(fmt.Sprintf("%p", b), Equals, fmt.Sprintf("%p", shadowB))
}
