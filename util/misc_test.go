// Copyright 2016 PingCAP, Inc.
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
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

func (s *testMiscSuite) SetUpSuite(c *C) {
}

func (s *testMiscSuite) TearDownSuite(c *C) {
}

func (s testMiscSuite) TestRunWithRetry(c *C) {
	defer testleak.AfterTest(c)()
	// Run succ.
	cnt := 0
	err := RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 2)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 4 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 3)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return false, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 1)
}
