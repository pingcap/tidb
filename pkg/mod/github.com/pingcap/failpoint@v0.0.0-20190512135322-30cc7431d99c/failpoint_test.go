package failpoint_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

func TestFailpoint(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&failpointSuite{})

type failpointSuite struct{}

func (s *failpointSuite) TestWithHook(c *C) {
	err := failpoint.Enable("TestWithHook-test-0", "return(1)")
	c.Assert(err, IsNil)

	val, ok := failpoint.EvalContext(context.Background(), "TestWithHook-test-0")
	c.Assert(val, IsNil)
	c.Assert(ok, IsFalse)

	val, ok = failpoint.EvalContext(nil, "TestWithHook-test-0")
	c.Assert(val, IsNil)
	c.Assert(ok, IsFalse)

	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return false
	})
	val, ok = failpoint.EvalContext(ctx, "unit-test")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	ctx = failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return true
	})
	err = failpoint.Enable("TestWithHook-test-1", "return(1)")
	c.Assert(err, IsNil)
	defer func() {
		err := failpoint.Disable("TestWithHook-test-1")
		c.Assert(err, IsNil)
	}()
	val, ok = failpoint.EvalContext(ctx, "TestWithHook-test-1")
	c.Assert(ok, IsTrue)
	c.Assert(val.(int), Equals, 1)
}
