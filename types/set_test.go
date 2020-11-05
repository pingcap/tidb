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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = SerialSuites(&testSetSuite{})

type testSetSuite struct {
}

func (s *testSetSuite) TestSet(c *C) {
	defer testleak.AfterTest(c)()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	elems := []string{"a", "b", "c", "d"}
	tbl := []struct {
		Name          string
		ExpectedValue uint64
		ExpectedName  string
	}{
		{"a", 1, "a"},
		{"a,b,a", 3, "a,b"},
		{"b,a", 3, "a,b"},
		{"a,b,c,d", 15, "a,b,c,d"},
		{"d", 8, "d"},
		{"", 0, ""},
		{"0", 0, ""},
	}
	citbl := []struct {
		Name          string
		ExpectedValue uint64
		ExpectedName  string
	}{
		{"A ", 1, "a"},
		{"a,B,a", 3, "a,b"},
	}

	for _, t := range tbl {
		e, err := ParseSetName(elems, t.Name, mysql.DefaultCollationName)
		c.Assert(err, IsNil)
		c.Assert(e.ToNumber(), Equals, float64(t.ExpectedValue))
		c.Assert(e.String(), Equals, t.ExpectedName)
	}

	for _, t := range tbl {
		e, err := ParseSetName(elems, t.Name, "utf8_unicode_ci")
		c.Assert(err, IsNil)
		c.Assert(e.ToNumber(), Equals, float64(t.ExpectedValue))
		c.Assert(e.String(), Equals, t.ExpectedName)
	}

	for _, t := range citbl {
		e, err := ParseSetName(elems, t.Name, "utf8_general_ci")
		c.Assert(err, IsNil)
		c.Assert(e.ToNumber(), Equals, float64(t.ExpectedValue))
		c.Assert(e.String(), Equals, t.ExpectedName)
	}

	tblNumber := []struct {
		Number       uint64
		ExpectedName string
	}{
		{0, ""},
		{1, "a"},
		{3, "a,b"},
		{9, "a,d"},
	}

	for _, t := range tblNumber {
		e, err := ParseSetValue(elems, t.Number)
		c.Assert(err, IsNil)
		c.Assert(e.String(), Equals, t.ExpectedName)
		c.Assert(e.ToNumber(), Equals, float64(t.Number))
	}

	tblErr := []string{
		"a.e",
		"e.f",
	}
	for _, t := range tblErr {
		_, err := ParseSetName(elems, t, mysql.DefaultCollationName)
		c.Assert(err, NotNil)
	}

	tblNumberErr := []uint64{
		100, 16, 64,
	}
	for _, t := range tblNumberErr {
		_, err := ParseSetValue(elems, t)
		c.Assert(err, NotNil)
	}
}
