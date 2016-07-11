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

package parser

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testAllocator{})

type testAllocator struct {
}

func (t *testAllocator) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	ac := newAllocator()

	ac.newFieldType(mysql.TypeBlob)
	ac.newFieldType(mysql.TypeDate)
	ac.newFieldType(mysql.TypeEnum)
	ac.reset()
	for i := 0; i < 3; i++ {
		ft := ac.allocFieldType()
		c.Assert(ft.Tp, Equals, byte(0))
	}
}
