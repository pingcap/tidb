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

package mysql

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"testing"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMySQLConstSuite{})

type testMySQLConstSuite struct {
}

func (s *testMySQLConstSuite) TestGetSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
		{" ,"},
	}

	for _, t := range positiveCases {
		_, err := GetSQLMode(t.arg)
		c.Assert(err, IsNil)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE, adfadsdfasdfads"},
	}

	for _, t := range negativeCases {
		_, err := GetSQLMode(t.arg)
		c.Assert(err, NotNil)
	}
}
