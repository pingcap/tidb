// Copyright 2017 PingCAP, Inc.
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

package testkit

import (
	"testing"

	"github.com/pingcap/check"
)

var _ = check.Suite(&testKitSuite{})

func TestT(t *testing.T) {
	check.TestingT(t)
}

type testKitSuite struct {
}

func (s testKitSuite) TestSort(c *check.C) {
	result := &Result{
		rows:    [][]string{{"1", "1", "<nil>", "<nil>"}, {"2", "2", "2", "3"}},
		c:       c,
		comment: check.Commentf(""),
	}
	result.Sort().Check(Rows("1 1 <nil> <nil>", "2 2 2 3"))
}
