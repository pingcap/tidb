// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package format

import (
	"bytes"
	"io/ioutil"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFormatSuite{})

type testFormatSuite struct {
}

func checkFormat(c *C, f Formatter, buf *bytes.Buffer, str, expect string) {
	_, err := f.Format(str, 3)
	c.Assert(err, IsNil)
	b, err := ioutil.ReadAll(buf)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, expect)
}

func (s *testFormatSuite) TestFormat(c *C) {
	defer testleak.AfterTest(c)()
	str := "abc%d%%e%i\nx\ny\n%uz\n"
	buf := &bytes.Buffer{}
	f := IndentFormatter(buf, "\t")
	expect := `abc3%e
	x
	y
z
`
	checkFormat(c, f, buf, str, expect)

	str = "abc%d%%e%i\nx\ny\n%uz\n%i\n"
	buf = &bytes.Buffer{}
	f = FlatFormatter(buf)
	expect = "abc3%e x y z\n "
	checkFormat(c, f, buf, str, expect)

	str2 := OutputFormat(`\'\000abc\n\rdef`)
	c.Assert(str2, Equals, "\\''\\000abc\\n\\rdef")
}
