package stringutil

import (
	. "github.com/pingcap/check"
)

func (s *testStringUtilSuite) TestSeparatedString(c *C) {
	builder := NewSeparatedString("; ")
	builder.Append("A: a")
	builder.Append("B: b")
	builder.StartNext()
	builder.WriteString("C: (")
	builder.StartScope(", ")
	{
		builder.Append("x")
		builder.Append("y")
		builder.Append("z")
	}
	builder.EndScope()
	builder.WriteString(")")
	builder.Append("D: d")

	c.Assert(builder.String(), Equals, "A: a; B: b; C: (x, y, z); D: d")
}
