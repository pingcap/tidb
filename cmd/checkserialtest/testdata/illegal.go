// Copyright 2020 PingCAP, Inc.
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

package testdata

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (ts *TestSuite) Test1(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/xxxxxxxxxxxx"), IsNil)
	}()
}

func (ts *TestSuite) Test2(c *C) {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func test3() {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func (ts *TestSuite) Test3(c *C) {
	test3()
}

func (ts *TestSuite) Test4(c *C) {
	go func() {
		failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
	}()
}
