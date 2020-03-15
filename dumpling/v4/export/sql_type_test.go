package export

import . "github.com/pingcap/check"

var _ = Suite(&testSqlByteSuite{})

type testSqlByteSuite struct{}

func (s *testSqlByteSuite) TestEscape(c *C) {
	str := `MWQeWw""'\rNmtGxzGp`
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\rNmtGxzGp`
	c.Assert(escape(str, true), Equals, expectStrBackslash)
	c.Assert(escape(str, false), Equals, expectStrWithoutBackslash)
}
