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
	"strings"
	"testing"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMySQLConstSuite{})

type testMySQLConstSuite struct {
}

func (s *testMySQLConstSuite) TestSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		arg                 string
		hasNoZeroDateMode   bool
		hasNoZeroInDateMode bool
	}{
		{"NO_ZERO_DATE", true, false},
		{"NO_ZERO_IN_DATE", false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true},
		{"", false, false},
	}

	for _, t := range tests {
		modes := strings.Split(t.arg, ",")
		var sqlMode SQLMode
		for _, mode := range modes {
			sqlMode = sqlMode | GetSQLMode(mode)
		}
		c.Assert(sqlMode.HasNoZeroDateMode(), Equals, t.hasNoZeroDateMode)
		c.Assert(sqlMode.HasNoZeroInDateMode(), Equals, t.hasNoZeroInDateMode)
	}
}
