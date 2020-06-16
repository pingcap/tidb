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

package testutil

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTestUtilSuite{})
var _ = Suite(&testCommonHandleSuite{})

type testTestUtilSuite struct{}

func (s *testTestUtilSuite) TestCompareUnorderedString(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		a []string
		b []string
		r bool
	}{
		{[]string{"1", "1", "2"}, []string{"1", "1", "2"}, true},
		{[]string{"1", "1", "2"}, []string{"1", "2", "1"}, true},
		{[]string{"1", "1"}, []string{"1", "2", "1"}, false},
		{[]string{"1", "1", "2"}, []string{"1", "2", "2"}, false},
		{nil, nil, true},
		{[]string{}, nil, false},
		{nil, []string{}, false},
	}
	for _, t := range tbl {
		c.Assert(CompareUnorderedStringSlice(t.a, t.b), Equals, t.r)
	}
}

type testCommonHandleSuite struct {
	CommonHandleSuite
	expectedIsCommonHandle bool
}

func (s *testCommonHandleSuite) TestCommonHandleSuiteRerun(c *C) {
	c.Assert(s.IsCommonHandle, Equals, s.expectedIsCommonHandle)
	hd := s.NewHandle().Int(1).Build()
	if s.expectedIsCommonHandle {
		c.Assert(hd.IsInt(), IsFalse)
	} else {
		c.Assert(hd.IsInt(), IsTrue)
	}
	s.expectedIsCommonHandle = true
	s.RerunWithCommonHandleEnabled(c, s.TestCommonHandleSuiteRerun)
}

func (s *testCommonHandleSuite) TestCommonHandleSuiteInitState(c *C) {
	c.Assert(s.IsCommonHandle, IsFalse)
}
