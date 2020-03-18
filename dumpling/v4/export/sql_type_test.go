package export

import (
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSqlByteSuite{})

type testSqlByteSuite struct{}

func (s *testSqlByteSuite) TestEscape(c *C) {
	var bf bytes.Buffer
	str := []byte(`MWQeWw""'\rNmtGxzGp`)
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\rNmtGxzGp`
	escape(str, &bf, true)
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escape(str, &bf, false)
	c.Assert(bf.String(), Equals, expectStrWithoutBackslash)
}
