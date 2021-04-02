// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

func (s *testMiscSuite) TestCompatibleParseGCTime(c *C) {
	values := []string{
		"20181218-19:53:37 +0800 CST",
		"20181218-19:53:37 +0800 MST",
		"20181218-19:53:37 +0800 FOO",
		"20181218-19:53:37 +0800 +08",
		"20181218-19:53:37 +0800",
		"20181218-19:53:37 +0800 ",
		"20181218-11:53:37 +0000",
	}

	invalidValues := []string{
		"",
		" ",
		"foo",
		"20181218-11:53:37",
		"20181218-19:53:37 +0800CST",
		"20181218-19:53:37 +0800 FOO BAR",
		"20181218-19:53:37 +0800FOOOOOOO BAR",
		"20181218-19:53:37 ",
	}

	expectedTime := time.Date(2018, 12, 18, 11, 53, 37, 0, time.UTC)
	expectedTimeFormatted := "20181218-19:53:37 +0800"

	beijing, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)

	for _, value := range values {
		t, err := CompatibleParseGCTime(value)
		c.Assert(err, IsNil)
		c.Assert(t.Equal(expectedTime), Equals, true)

		formatted := t.In(beijing).Format(GCTimeFormat)
		c.Assert(formatted, Equals, expectedTimeFormatted)
	}

	for _, value := range invalidValues {
		_, err := CompatibleParseGCTime(value)
		c.Assert(err, NotNil)
	}
}
