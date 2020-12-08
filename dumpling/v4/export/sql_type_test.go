// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSQLByteSuite{})

type testSQLByteSuite struct{}

func (s *testSQLByteSuite) TestEscape(c *C) {
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
