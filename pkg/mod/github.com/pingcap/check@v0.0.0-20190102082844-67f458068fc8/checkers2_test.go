package check_test

import (
	"time"

	"github.com/pingcap/check"
)

func testHasKey(c *check.C, expectedResult bool, expectedErr string, params ...interface{}) {
	actualResult, actualErr := check.HasKey.Check(params, nil)
	if actualResult != expectedResult || actualErr != expectedErr {
		c.Fatalf(
			"Check returned (%#v, %#v) rather than (%#v, %#v)",
			actualResult, actualErr, expectedResult, expectedErr)
	}
}

func (s *CheckersS) TestHasKey(c *check.C) {
	testHasKey(c, true, "", map[string]int{"foo": 1}, "foo")
	testHasKey(c, false, "", map[string]int{"foo": 1}, "bar")
	testHasKey(c, true, "", map[int][]byte{10: nil}, 10)

	testHasKey(c, false, "First argument to HasKey must be a map", nil, "bar")
	testHasKey(
		c, false, "Second argument must be assignable to the map key type",
		map[string]int{"foo": 1}, 10)
}

func (s *CheckersS) TestCompare(c *check.C) {
	c.Assert(10, check.Less, 11)
	c.Assert(10, check.LessEqual, 10)
	c.Assert(10, check.Greater, 9)
	c.Assert(10, check.GreaterEqual, 10)
	c.Assert(10, check.Not(check.LessEqual), 9)
	c.Assert(10, check.Not(check.Less), 9)
	c.Assert("ABC", check.Less, "ABCD")
	c.Assert([]byte("ABC"), check.Less, []byte("ABCD"))
	c.Assert(3.14, check.Less, 3.145)
	c.Assert(time.Duration(1), check.Greater, time.Duration(0))
	c.Assert(time.Now(), check.Less, time.Now().Add(10*time.Second))
}

func (s *CheckersS) TestBytes(c *check.C) {
	c.Assert([]byte{0x00}, check.BytesEquals, []byte{0x00})
	c.Assert([]byte{0x00}, check.Not(check.BytesEquals), []byte{0x01})
}
