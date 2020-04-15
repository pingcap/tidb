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

package printer

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrinterSuite{})

type testPrinterSuite struct {
}

func (s *testPrinterSuite) TestPrintResult(c *C) {
	defer testleak.AfterTest(c)()
	cols := []string{"col1", "col2", "col3"}
	datas := [][]string{{"11"}, {"21", "22", "23"}}
	result, ok := GetPrintResult(cols, datas)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")

	datas = [][]string{{"11", "12", "13"}, {"21", "22", "23"}}
	expect := `
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 11   | 12   | 13   |
| 21   | 22   | 23   |
+------+------+------+
`
	result, ok = GetPrintResult(cols, datas)
	c.Assert(ok, IsTrue)
	c.Assert(result, Equals, expect[1:])

	datas = nil
	result, ok = GetPrintResult(cols, datas)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")

	cols = nil
	result, ok = GetPrintResult(cols, datas)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")
}
