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
	expectStrBackslashDoubleQuote := `MWQeWw""""'\rNmtGxzGp`
	escape(str, &bf, getEscapeQuotation(true, quotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(true, doubleQuotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(false, quotationMark))
	c.Assert(bf.String(), Equals, expectStrWithoutBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(false, doubleQuotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslashDoubleQuote)
}
